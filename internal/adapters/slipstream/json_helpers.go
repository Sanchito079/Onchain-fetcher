package slipstream

import (
	"encoding/json"
	"fmt"
	"io"
)

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("json marshal failed: %v", err))
	}
	return string(data)
}

func decodeJSON(r io.Reader, out any) error {
	return json.NewDecoder(r).Decode(out)
}
