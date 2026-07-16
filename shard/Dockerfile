FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o shard .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/shard .
EXPOSE 8081
CMD ["./shard"]
