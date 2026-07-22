FROM golang:1.26-alpine AS build
WORKDIR /app
COPY coordinator/go.mod coordinator/go.sum ./
RUN go mod download
COPY coordinator/ .
RUN CGO_ENABLED=0 GOOS=linux go build -o coordinator .

FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/coordinator .
COPY --from=frontend-build /app/frontend/dist ./frontend/dist
EXPOSE 8080
CMD ["./coordinator"]
