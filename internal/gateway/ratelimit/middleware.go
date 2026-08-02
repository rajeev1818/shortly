package ratelimit

import (
	"net"
	"net/http"
)

func Middleware(limiter *Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr // fallback if parsing fails
			}
			key := "ratelimit:" + ip
			allowed, err := limiter.Allow(r.Context(), key)
			if err != nil {
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				w.Header().Set("Retry-After", "60")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
