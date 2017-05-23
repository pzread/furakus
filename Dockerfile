FROM alpine:edge
RUN apk update && apk upgrade
RUN apk add --update rust cargo file make
WORKDIR /furakus
CMD ["cargo", "run", "--release"]
