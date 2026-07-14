# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /out/kore-agent ./cmd/kore-agent \
 && CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /out/kore-scheduler ./cmd/kore-scheduler \
 && CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /out/kore-operator ./cmd/kore-operator \
 && CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /out/kore-kueue-admissioncheck ./cmd/kore-kueue-admissioncheck

FROM gcr.io/distroless/static:nonroot AS scheduler
COPY --from=build /out/kore-scheduler /kore-scheduler
ENTRYPOINT ["/kore-scheduler"]

FROM gcr.io/distroless/static:nonroot AS operator
COPY --from=build /out/kore-operator /kore-operator
ENTRYPOINT ["/kore-operator"]

FROM gcr.io/distroless/static:nonroot AS admissioncheck
COPY --from=build /out/kore-kueue-admissioncheck /kore-kueue-admissioncheck
ENTRYPOINT ["/kore-kueue-admissioncheck"]

# agent 需要 root：写 NRI socket、device plugin socket
FROM gcr.io/distroless/static:latest AS agent
COPY --from=build /out/kore-agent /kore-agent
ENTRYPOINT ["/kore-agent"]
