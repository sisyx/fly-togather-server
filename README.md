# Fly Together API

Go HTTP and native WebSocket server. Run with `go run .` and test with `go test ./...`.

Environment:

- `PORT`: listen port, default `3001`.
- `DATA_DIR`: directory for `uploads/`, `covers/`, and `saved-tracks.json`; default current working directory.

Routes: `/api/*`, `/upload`, `/uploads/*`, `/covers/*`, and `/ws`. The WebSocket protocol sends JSON objects with `event` and `data` fields. The server allows cross-origin requests for independently hosted frontends.

The existing media and library files are local data. Copy them with this project when moving it to another server; they are intentionally ignored by Git.
