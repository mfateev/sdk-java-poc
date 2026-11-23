use bytes::{Buf, BufMut, Bytes};
use futures_util::future::BoxFuture;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::{
    collections::HashMap,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicI64, Ordering},
    },
};
use temporalio_client::callback_based::{GrpcRequest, GrpcSuccessResponse};
use tokio::sync::oneshot;
use tonic::{Code, Status};

type Reply = Result<GrpcSuccessResponse, Status>;
static NEXT_RPC: AtomicI64 = AtomicI64::new(1);
static RPCS: LazyLock<Mutex<HashMap<i64, oneshot::Sender<Reply>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

unsafe extern "C" {
    #[link_name = "jvm:static:io/temporal/internal/corebridge/JavaHost:startRpc"]
    fn start_rpc(context: i64, token: i64, request: *const u8, length: i32) -> bool;

    #[link_name = "jvm:static:io/temporal/internal/corebridge/JavaHost:cancelRpc"]
    fn cancel_rpc(context: i64, token: i64);
}

pub(crate) fn put_field(output: &mut Vec<u8>, field: &[u8]) -> Result<(), Status> {
    let length = u32::try_from(field.len())
        .map_err(|_| Status::resource_exhausted("bridge field too large"))?;
    output.put_u32(length);
    output.extend_from_slice(field);
    Ok(())
}

pub(crate) fn take_field<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], Status> {
    let length = input
        .try_get_u32()
        .map_err(|_| Status::internal("missing field length"))? as usize;
    let (field, rest) = input
        .split_at_checked(length)
        .ok_or_else(|| Status::internal("truncated bridge field"))?;
    *input = rest;
    Ok(field)
}

fn encode_request(request: GrpcRequest) -> Result<Vec<u8>, Status> {
    let mut data = Vec::new();
    put_field(&mut data, request.service.as_bytes())?;
    put_field(&mut data, request.rpc.as_bytes())?;
    data.put_u32(request.headers.len() as u32);
    for (key, value) in &request.headers {
        put_field(&mut data, key.as_str().as_bytes())?;
        put_field(&mut data, value.as_bytes())?;
    }
    put_field(&mut data, &request.proto)?;
    Ok(data)
}

fn decode_response(mut data: &[u8]) -> Reply {
    let code = Code::from(
        data.try_get_i32()
            .map_err(|_| Status::internal("missing gRPC code"))?,
    );
    let message = take_field(&mut data)?;
    let details = take_field(&mut data)?;
    let count = data
        .try_get_u32()
        .map_err(|_| Status::internal("missing header count"))?;
    let mut headers = HeaderMap::new();
    for _ in 0..count {
        let key = HeaderName::from_bytes(take_field(&mut data)?)
            .map_err(|_| Status::internal("invalid response header name"))?;
        let value = HeaderValue::from_bytes(take_field(&mut data)?)
            .map_err(|_| Status::internal("invalid response header value"))?;
        headers.append(key, value);
    }
    let proto = take_field(&mut data)?.to_vec();
    if code == Code::Ok {
        Ok(GrpcSuccessResponse { headers, proto })
    } else {
        let message = String::from_utf8_lossy(message).into_owned();
        Err(Status::with_details_and_metadata(
            code,
            message,
            Bytes::copy_from_slice(details),
            tonic::metadata::MetadataMap::from_headers(headers),
        ))
    }
}

struct PendingRpc {
    context: i64,
    token: i64,
}

impl Drop for PendingRpc {
    fn drop(&mut self) {
        let pending = RPCS.lock().unwrap().remove(&self.token).is_some();
        if pending {
            unsafe { cancel_rpc(self.context, self.token) };
        }
    }
}

pub(crate) fn send(context: i64, request: GrpcRequest) -> BoxFuture<'static, Reply> {
    Box::pin(async move {
        let envelope = encode_request(request)?;
        let length = i32::try_from(envelope.len())
            .map_err(|_| Status::resource_exhausted("RPC too large"))?;
        let token = NEXT_RPC.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        RPCS.lock().unwrap().insert(token, sender);
        let _pending = PendingRpc { context, token };
        if !unsafe { start_rpc(context, token, envelope.as_ptr(), length) } {
            return Err(Status::unavailable("Java gRPC host is unavailable"));
        }
        receiver
            .await
            .map_err(|_| Status::cancelled("Java transport dropped the response"))?
    })
}

pub(crate) fn finish(token: i64, response: &[u8]) {
    let pending = RPCS.lock().unwrap().remove(&token);
    if let Some(sender) = pending {
        let _ = sender.send(decode_response(response));
    }
}
