package services

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dhowden/tag"
	"github.com/gorilla/websocket"
)

type Metadata struct {
	Title    string `json:"title"`
	Artist   string `json:"artist,omitempty"`
	Album    string `json:"album,omitempty"`
	Year     string `json:"year,omitempty"`
	CoverURL string `json:"coverUrl,omitempty"`
}
type Track struct {
	Filename     string   `json:"filename"`
	OriginalName string   `json:"originalName"`
	URL          string   `json:"url"`
	Metadata     Metadata `json:"metadata"`
}
type SavedTrack struct {
	ID           string   `json:"id"`
	Filename     string   `json:"filename"`
	OriginalName string   `json:"originalName"`
	URL          string   `json:"url"`
	SavedAt      string   `json:"savedAt"`
	Metadata     Metadata `json:"metadata"`
	Likes        []string `json:"likes,omitempty"`
}
type State struct {
	Track       *Track  `json:"track"`
	Playing     bool    `json:"playing"`
	CurrentTime float64 `json:"currentTime"`
	Duration    float64 `json:"duration"`
}
type Message struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data,omitempty"`
}
type client struct {
	id   string
	conn *websocket.Conn
	send chan Message
}
type app struct {
	mu        sync.Mutex
	dataDir   string
	savedFile string
	state     State
	clients   map[*client]bool
	host      *client
	upgrader  websocket.Upgrader
}

func NewApp(dataDir string) (*app, error) {
	for _, dir := range []string{"uploads", "covers"} {
		if err := os.MkdirAll(filepath.Join(dataDir, dir), 0755); err != nil {
			return nil, err
		}
	}
	return &app{dataDir: dataDir, savedFile: filepath.Join(dataDir, "saved-tracks.json"), clients: make(map[*client]bool), upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}, nil
}
func (a *app) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", a.getState)
	mux.HandleFunc("GET /api/saved", a.getSaved)
	mux.HandleFunc("POST /api/saved/{id}/like", a.likeSaved)
	mux.HandleFunc("DELETE /api/saved/{id}", a.deleteSaved)
	mux.HandleFunc("POST /api/saved/{id}/set-current", a.setCurrent)
	mux.HandleFunc("GET /api/saved/{id}/download", a.download)
	mux.HandleFunc("GET /api/metadata/{filename}", a.getMetadata)
	mux.HandleFunc("POST /upload", a.upload)
	mux.HandleFunc("GET /ws", a.websocket)
	mux.Handle("GET /uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir(filepath.Join(a.dataDir, "uploads")))))
	mux.Handle("GET /covers/", http.StripPrefix("/covers/", http.FileServer(http.Dir(filepath.Join(a.dataDir, "covers")))))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": message})
}
func (a *app) readSaved() []SavedTrack {
	bytes, err := os.ReadFile(a.savedFile)
	if err != nil {
		return []SavedTrack{}
	}
	var tracks []SavedTrack
	if json.Unmarshal(bytes, &tracks) != nil || tracks == nil {
		return []SavedTrack{}
	}
	return tracks
}
func (a *app) writeSaved(tracks []SavedTrack) error {
	bytes, err := json.MarshalIndent(tracks, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(a.dataDir, ".saved-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err = temp.Write(bytes); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), a.savedFile)
}
func randomID() string { var b [12]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func (a *app) addSaved(track Track) error {
	tracks := a.readSaved()
	for _, t := range tracks {
		if t.Filename == track.Filename {
			return nil
		}
	}
	tracks = append(tracks, SavedTrack{ID: randomID(), Filename: track.Filename, OriginalName: track.OriginalName, URL: track.URL, SavedAt: time.Now().UTC().Format(time.RFC3339), Metadata: track.Metadata})
	return a.writeSaved(tracks)
}
func (a *app) getState(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	jsonResponse(w, 200, a.state)
}
func (a *app) getSaved(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	jsonResponse(w, 200, map[string]any{"saved": a.readSaved()})
}
func (a *app) findSaved(id string) (SavedTrack, bool) {
	for _, t := range a.readSaved() {
		if t.ID == id {
			return t, true
		}
	}
	return SavedTrack{}, false
}
func (a *app) likeSaved(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body) != nil {
		fail(w, 400, "A valid listener name is required")
		return
	}
	name := strings.Join(strings.Fields(body.Name), " ")
	if len([]rune(name)) > 40 {
		name = string([]rune(name)[:40])
	}
	if len([]rune(name)) < 2 {
		fail(w, 400, "A valid listener name is required")
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tracks := a.readSaved()
	for i := range tracks {
		if tracks[i].ID == r.PathValue("id") {
			liked := true
			for j, existing := range tracks[i].Likes {
				if strings.EqualFold(existing, name) {
					tracks[i].Likes = append(tracks[i].Likes[:j], tracks[i].Likes[j+1:]...)
					liked = false
					break
				}
			}
			if liked {
				tracks[i].Likes = append(tracks[i].Likes, name)
			}
			if err := a.writeSaved(tracks); err != nil {
				fail(w, 500, err.Error())
				return
			}
			likes := tracks[i].Likes
			if likes == nil {
				likes = []string{}
			}
			a.broadcast("track-liked", map[string]any{"trackId": tracks[i].ID, "likes": likes}, nil)
			jsonResponse(w, 200, map[string]any{"liked": liked, "likeCount": len(likes), "likes": likes})
			return
		}
	}
	fail(w, 404, "Saved track not found")
}
func (a *app) deleteSaved(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	tracks := a.readSaved()
	for i, t := range tracks {
		if t.ID == r.PathValue("id") {
			tracks = append(tracks[:i], tracks[i+1:]...)
			if err := a.writeSaved(tracks); err != nil {
				fail(w, 500, err.Error())
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	fail(w, 404, "Saved track not found")
}
func safeFilename(name string) bool {
	return name != "" && name == filepath.Base(name) && name != "." && name != ".." && !strings.ContainsAny(name, "/\\")
}
func (a *app) audioPath(filename string) (string, error) {
	if !safeFilename(filename) {
		return "", errors.New("invalid filename")
	}
	return filepath.Join(a.dataDir, "uploads", filename), nil
}
func (a *app) setCurrent(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.findSaved(r.PathValue("id"))
	if !ok {
		fail(w, 404, "Saved track not found")
		return
	}
	path, err := a.audioPath(entry.Filename)
	if err != nil {
		fail(w, 404, "Audio file no longer available")
		return
	}
	if _, err = os.Stat(path); err != nil {
		fail(w, 404, "Audio file no longer available")
		return
	}
	metadata := entry.Metadata
	if metadata.Title == "" {
		metadata = a.extractMetadata(path, entry.OriginalName)
	}
	track := Track{Filename: entry.Filename, OriginalName: entry.OriginalName, URL: "/uploads/" + entry.Filename, Metadata: metadata}
	a.selectTrack(track)
	jsonResponse(w, 200, track)
}
func (a *app) selectTrack(track Track) {
	a.state = State{Track: &track}
	a.broadcast("track-changed", track, nil)
}
func (a *app) download(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	entry, ok := a.findSaved(r.PathValue("id"))
	a.mu.Unlock()
	if !ok {
		fail(w, 404, "Saved track not found")
		return
	}
	path, err := a.audioPath(entry.Filename)
	if err != nil {
		fail(w, 404, "Audio file no longer available")
		return
	}
	if _, err = os.Stat(path); err != nil {
		fail(w, 404, "Audio file no longer available")
		return
	}
	name := strings.Map(func(ch rune) rune {
		if ch == '"' || ch == '\\' || ch == '\r' || ch == '\n' {
			return '_'
		}
		return ch
	}, entry.OriginalName)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	http.ServeFile(w, r, path)
}
func (a *app) getMetadata(w http.ResponseWriter, r *http.Request) {
	filename := r.PathValue("filename")
	path, err := a.audioPath(filename)
	if err != nil {
		fail(w, 404, "File not found")
		return
	}
	if _, err = os.Stat(path); err != nil {
		fail(w, 404, "File not found")
		return
	}
	a.mu.Lock()
	name := filename
	for _, t := range a.readSaved() {
		if t.Filename == filename {
			name = t.OriginalName
			break
		}
	}
	a.mu.Unlock()
	jsonResponse(w, 200, map[string]any{"filename": filename, "metadata": a.extractMetadata(path, name)})
}
func (a *app) extractMetadata(path, name string) Metadata {
	ext := filepath.Ext(name)
	meta := Metadata{Title: strings.TrimSuffix(filepath.Base(name), ext)}
	if strings.ToLower(filepath.Ext(path)) != ".mp3" {
		return meta
	}
	file, err := os.Open(path)
	if err != nil {
		return meta
	}
	defer file.Close()
	tags, err := tag.ReadFrom(file)
	if err != nil {
		return meta
	}
	if tags.Title() != "" {
		meta.Title = tags.Title()
	}
	meta.Artist = tags.Artist()
	meta.Album = tags.Album()
	if tags.Year() != 0 {
		meta.Year = strconv.Itoa(tags.Year())
	}
	if picture := tags.Picture(); picture != nil && len(picture.Data) > 0 {
		ext := "." + strings.TrimPrefix(strings.ToLower(picture.Ext), ".")
		if ext != ".png" && ext != ".gif" && ext != ".webp" {
			ext = ".jpg"
		}
		coverName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)) + ext
		coverPath := filepath.Join(a.dataDir, "covers", coverName)
		if _, err := os.Stat(coverPath); os.IsNotExist(err) {
			_ = os.WriteFile(coverPath, picture.Data, 0644)
		}
		meta.CoverURL = "/covers/" + coverName
	}
	return meta
}
func (a *app) upload(w http.ResponseWriter, r *http.Request) {
	const maxSize = 50 << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxSize+(1<<20))
	if err := r.ParseMultipartForm(maxSize); err != nil {
		fail(w, 400, "Upload exceeds 50MB or is invalid")
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	file, header, err := r.FormFile("audio")
	if err != nil {
		fail(w, 400, "No file uploaded")
		return
	}
	defer file.Close()
	name := filepath.Base(header.Filename)
	ext := strings.ToLower(filepath.Ext(name))
	if ext != ".mp3" && ext != ".wav" && ext != ".ogg" && ext != ".m4a" {
		fail(w, 400, "Only mp3, wav, ogg, m4a are allowed")
		return
	}
	filename := "track_" + randomID() + ext
	path := filepath.Join(a.dataDir, "uploads", filename)
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		fail(w, 500, err.Error())
		return
	}
	n, err := io.Copy(out, io.LimitReader(file, maxSize+1))
	closeErr := out.Close()
	if err != nil || closeErr != nil || n > maxSize {
		os.Remove(path)
		fail(w, 400, "Upload exceeds 50MB or is invalid")
		return
	}
	track := Track{Filename: filename, OriginalName: name, URL: "/uploads/" + filename, Metadata: a.extractMetadata(path, name)}
	a.mu.Lock()
	err = a.addSaved(track)
	if err == nil {
		a.selectTrack(track)
	}
	a.mu.Unlock()
	if err != nil {
		os.Remove(path)
		fail(w, 500, err.Error())
		return
	}
	jsonResponse(w, 200, map[string]any{"url": track.URL, "originalName": track.OriginalName, "metadata": track.Metadata})
}
func message(event string, data any) Message {
	bytes, _ := json.Marshal(data)
	return Message{Event: event, Data: bytes}
}
func (a *app) send(c *client, event string, data any) {
	select {
	case c.send <- message(event, data):
	default:
	}
}

// broadcast is called while a.mu is held.
func (a *app) broadcast(event string, data any, except *client) {
	for c := range a.clients {
		if c != except {
			a.send(c, event, data)
		}
	}
}
func (a *app) roles() {
	for c := range a.clients {
		hostID := ""
		if a.host != nil {
			hostID = a.host.id
		}
		a.send(c, "role", map[string]any{"isHost": c == a.host, "hostId": hostID})
	}
}
func (a *app) websocket(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{id: randomID(), conn: conn, send: make(chan Message, 32)}
	a.mu.Lock()
	a.clients[c] = true
	if a.host == nil {
		a.host = c
	}
	a.send(c, "state-sync", a.state)
	a.roles()
	a.broadcast("users-count", len(a.clients), nil)
	a.mu.Unlock()
	go func() {
		for msg := range c.send {
			if err := c.conn.WriteJSON(msg); err != nil {
				return
			}
		}
	}()
	defer func() {
		a.mu.Lock()
		delete(a.clients, c)
		if a.host == c {
			a.host = nil
			for next := range a.clients {
				a.host = next
				break
			}
		}
		a.roles()
		a.broadcast("users-count", len(a.clients), nil)
		a.mu.Unlock()
		close(c.send)
		conn.Close()
	}()
	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		a.handleMessage(c, msg)
	}
}
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 }
func (a *app) handleMessage(c *client, msg Message) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch msg.Event {
	case "play":
		a.state.Playing = true
		a.broadcast("play", map[string]any{"currentTime": a.state.CurrentTime}, c)
	case "pause":
		a.state.Playing = false
		a.broadcast("pause", map[string]any{"currentTime": a.state.CurrentTime}, c)
	case "seek":
		var t float64
		if json.Unmarshal(msg.Data, &t) == nil && finite(t) {
			a.state.CurrentTime = t
			a.broadcast("seek", t, c)
		}
	case "position":
		if c != a.host {
			return
		}
		var p struct {
			CurrentTime float64 `json:"currentTime"`
			Duration    float64 `json:"duration"`
		}
		if json.Unmarshal(msg.Data, &p) == nil {
			if finite(p.CurrentTime) {
				a.state.CurrentTime = p.CurrentTime
			}
			if finite(p.Duration) {
				a.state.Duration = p.Duration
			}
			a.broadcast("position-sync", map[string]float64{"currentTime": a.state.CurrentTime, "duration": a.state.Duration}, c)
		}
	}
}
