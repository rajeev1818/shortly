package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	shortenerv1 "github.com/rajeev1818/shortly/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	client shortenerv1.ShortenerServiceClient
}

func NewHandler(client shortenerv1.ShortenerServiceClient) *Handler {
	return &Handler{
		client: client,
	}
}

func (h *Handler) Shorten(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)

	defer cancel()

	resp, err := h.client.Shorten(ctx, &shortenerv1.ShortenRequest{LongUrl: req.URL})

	if err != nil {
		st := status.Convert(err)
		switch st.Code() {
		case codes.Unavailable:
			writeError(w, "service unavailable", http.StatusServiceUnavailable)
		case codes.Internal:
			writeError(w, "internal server error", http.StatusInternalServerError)
		default:
			writeError(w, st.Message(), http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"short_code": resp.ShortCode})
}

func (h *Handler) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	if code == "" {
		writeError(w, "code is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.client.Resolve(ctx, &shortenerv1.ResolveRequest{ShortCode: code})

	if err != nil {
		st := status.Convert(err)

		switch st.Code() {
		case codes.NotFound:
			writeError(w, st.Message(), http.StatusNotFound)
		case codes.Unavailable:
			writeError(w, "service unavailable", http.StatusServiceUnavailable)
		default:
			writeError(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	http.Redirect(w, r, resp.LongUrl, http.StatusFound)
}

func writeError(w http.ResponseWriter, msg string, status int) {
	http.Error(w, msg, status)
}
