// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::client::{
    ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse,
    HttpResponseBody, HttpService,
};
use crate::{ObjectStoreExt, RetryConfig};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
struct Transport {
    status: u16,
    calls: Arc<AtomicUsize>,
}

impl HttpConnector for Transport {
    fn connect(&self, _: &ClientOptions) -> Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait]
impl HttpService for Transport {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        assert_eq!(request.method(), Method::DELETE);
        assert_eq!(request.uri().path(), "/container/owned/a%20b.lance");
        assert!(request.uri().query().is_none());
        assert_eq!(request.headers()["authorization"], "Bearer test-token");
        // Match bulk deletion's snapshot semantics; do not broaden deletion.
        assert!(!request.headers().contains_key("x-ms-delete-snapshots"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let body = HttpResponseBody::new(request.into_body()).bytes().await?;
        assert!(body.is_empty());
        let mut response = HttpResponse::new(HttpResponseBody::from(""));
        *response.status_mut() = self.status.try_into().unwrap();
        Ok(response)
    }
}

#[tokio::test]
async fn singleton_uses_native_delete_and_preserves_errors() {
    for status in [202, 404, 403, 503] {
        let calls = Arc::new(AtomicUsize::new(0));
        let store = MicrosoftAzureBuilder::new()
            .with_account("account")
            .with_container_name("container")
            .with_bearer_token_authorization("test-token")
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_http_connector(Transport {
                status,
                calls: calls.clone(),
            })
            .build()
            .unwrap();
        let result = store.delete(&Path::from("owned/a b.lance")).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        match status {
            202 => assert!(result.is_ok()),
            404 => assert!(matches!(result, Err(crate::Error::NotFound { .. }))),
            403 => assert!(matches!(result, Err(crate::Error::PermissionDenied { .. }))),
            _ => assert!(result.is_err()),
        }
    }
}
