package handler

import (
	"encoding/json"
	"net/http"

	"github.com/rajeev1818/shortly/internal/shortener/service"
)

type Handler struct {
	svc *service.URLService
}

func NewHandler(svc *service.URLService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Shorten(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	url, err := h.svc.Shorten(r.Context(), req.URL)

	if err != nil {
		writeError(w, "Error generating url", http.StatusInternalServerError)
		return
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"short_code": url})
	}
}

func (h *Handler) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	val, err := h.svc.GetByCode(r.Context(), code)

	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)

	} else {
		http.Redirect(w, r, val.LongURL, http.StatusFound)
	}
}

func writeError(w http.ResponseWriter, msg string, status int) {
	http.Error(w, msg, status)
}
