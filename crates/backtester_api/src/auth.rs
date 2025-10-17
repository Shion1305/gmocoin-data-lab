//! Optional bearer authentication for gRPC requests.

use std::sync::Arc;

use tonic::{Request, Status};

/// Interceptor enforcing a static bearer token when configured.
#[derive(Clone, Default)]
pub struct BearerInterceptor {
    token: Option<Arc<String>>,
}

impl BearerInterceptor {
    /// Builds a new interceptor; if `token` is `None`, authentication is disabled.
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: token.map(Arc::new),
        }
    }

    /// Validates an incoming request, returning a sanitized request or error status.
    pub fn intercept<T>(&self, request: Request<T>) -> Result<Request<T>, Status> {
        let Some(expected) = &self.token else {
            return Ok(request);
        };

        let Some(header) = request.metadata().get("authorization") else {
            return Err(Status::unauthenticated("missing authorization header"));
        };

        let value = header
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization header"))?;
        let mut parts = value.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some(scheme), Some(token))
                if scheme.eq_ignore_ascii_case("bearer") && token == expected.as_str() =>
            {
                Ok(request)
            }
            _ => Err(Status::unauthenticated("invalid bearer token")),
        }
    }
}
