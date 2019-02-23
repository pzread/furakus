use headers;
use hyper::http::{
    header::{HeaderName, HeaderValue},
    response::Builder as ResponseBuilder,
};

pub trait HeaderBuilderExt {
    fn typed_header<H: headers::Header>(&mut self, header: H) -> &mut Self;
}

impl HeaderBuilderExt for ResponseBuilder {
    fn typed_header<H: headers::Header>(&mut self, header: H) -> &mut Self {
        header.encode(&mut Extender {
            name: H::name(),
            builder: self,
        });
        self
    }
}

struct Extender<'a> {
    name: &'a HeaderName,
    builder: &'a mut ResponseBuilder,
}

impl<'a> Extend<HeaderValue> for Extender<'a> {
    fn extend<T: IntoIterator<Item = HeaderValue>>(&mut self, iter: T) {
        for value in iter {
            self.builder.header(self.name.clone(), value);
        }
    }
}
