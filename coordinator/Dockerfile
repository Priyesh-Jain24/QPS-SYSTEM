FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o coordinator .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/coordinator .
EXPOSE 8080
CMD ["./coordinator"]
