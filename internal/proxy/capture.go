package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"strings"
	"sync"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

type captureContextKey struct{}

func captureFromContext(ctx interface{ Value(any) any }) *responseCapture {
	c, _ := ctx.Value(captureContextKey{}).(*responseCapture)
	return c
}

type responseCapture struct {
	id     string
	store  types.ActivityStore
	ctx    context.Context
	status int
	noBody bool
	parser responseParser
	once   sync.Once
}

func (c *responseCapture) emit(delta types.ActivityDelta) {
	c.store.Append(c.id, delta, time.Now())
}

func (c *responseCapture) complete() {
	c.once.Do(func() {
		if c.parser != nil {
			c.parser.Finish()
		}
		if c.status >= 400 {
			c.store.Fail(c.id, fmt.Sprintf("upstream returned HTTP %d", c.status), false, time.Now())
			return
		}
		c.store.Complete(c.id, time.Now())
	})
}

func (c *responseCapture) fail(message string, canceled bool) {
	c.once.Do(func() {
		if c.parser != nil {
			c.parser.Finish()
		}
		c.store.Fail(c.id, message, canceled, time.Now())
	})
}

type observedBody struct {
	io.ReadCloser
	capture *responseCapture
	sawEOF  bool
}

func (b *observedBody) Close() error {
	err := b.ReadCloser.Close()
	if err != nil {
		b.capture.fail(err.Error(), false)
	} else if b.capture.noBody {
		b.capture.complete()
	} else if !b.sawEOF {
		b.capture.fail("upstream response closed before EOF", b.capture.ctx != nil && b.capture.ctx.Err() != nil)
	}
	return err
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 && b.capture.parser != nil {
		b.capture.parser.Feed(p[:n])
	}
	if err == io.EOF {
		b.sawEOF = true
		b.capture.complete()
	} else if err != nil {
		b.capture.fail(err.Error(), b.capture.ctx != nil && b.capture.ctx.Err() != nil)
	}
	return n, err
}

type responseParser interface {
	Feed([]byte)
	Finish()
}

func newResponseParser(contentType string, emit func(types.ActivityDelta)) responseParser {
	mediaType, _, _ := mime.ParseMediaType(contentType)
	switch mediaType {
	case "text/event-stream":
		return &sseParser{emit: emit}
	case "application/json", "application/problem+json":
		return &jsonParser{emit: emit}
	default:
		return &textParser{emit: emit}
	}
}

type textParser struct{ emit func(types.ActivityDelta) }

func (p *textParser) Feed(b []byte) { p.emit(types.ActivityDelta{Text: string(b)}) }
func (p *textParser) Finish()       {}

type jsonParser struct {
	buf  bytes.Buffer
	emit func(types.ActivityDelta)
}

func (p *jsonParser) Feed(b []byte) {
	const maxCapture = 8 << 20
	if p.buf.Len() >= maxCapture {
		return
	}
	remaining := maxCapture - p.buf.Len()
	p.buf.Write(b[:min(len(b), remaining)])
}

func (p *jsonParser) Finish() {
	emitPayload(bytes.TrimSpace(p.buf.Bytes()), p.emit)
}

type sseParser struct {
	buf  bytes.Buffer
	emit func(types.ActivityDelta)
}

func (p *sseParser) Feed(b []byte) {
	p.buf.Write(b)
	for {
		line, err := p.buf.ReadString('\n')
		if err != nil {
			p.buf.WriteString(line)
			return
		}
		p.processLine(line)
	}
}

func (p *sseParser) Finish() {
	if p.buf.Len() > 0 {
		p.processLine(p.buf.String())
		p.buf.Reset()
	}
}

func (p *sseParser) processLine(line string) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return
	}
	data := bytes.TrimSpace([]byte(strings.TrimPrefix(line, "data:")))
	if bytes.Equal(data, []byte("[DONE]")) {
		return
	}
	emitPayload(data, p.emit)
}

func emitPayload(raw []byte, emit func(types.ActivityDelta)) {
	if len(raw) == 0 {
		return
	}
	var payload struct {
		Type    string          `json:"type"`
		Delta   json.RawMessage `json:"delta"`
		Name    string          `json:"name"`
		ItemID  string          `json:"item_id"`
		Usage   json.RawMessage `json:"usage"`
		Choices []struct {
			Delta   completionMessage `json:"delta"`
			Message completionMessage `json:"message"`
		} `json:"choices"`
		Output []struct {
			Type      string `json:"type"`
			ID        string `json:"id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
			Content   []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return
	}
	for _, choice := range payload.Choices {
		emitMessage(choice.Delta, emit)
		emitMessage(choice.Message, emit)
	}
	for i, item := range payload.Output {
		for _, content := range item.Content {
			if content.Text != "" {
				emit(types.ActivityDelta{Text: content.Text})
			}
		}
		if item.Type == "function_call" {
			emit(types.ActivityDelta{ToolCall: &types.ToolCall{Index: i, ID: item.ID, Name: item.Name, Arguments: item.Arguments}})
		}
	}
	switch payload.Type {
	case "response.output_text.delta":
		var text string
		if json.Unmarshal(payload.Delta, &text) == nil && text != "" {
			emit(types.ActivityDelta{Text: text})
		}
	case "response.function_call_arguments.delta":
		var args string
		if json.Unmarshal(payload.Delta, &args) == nil {
			emit(types.ActivityDelta{ToolCall: &types.ToolCall{ID: payload.ItemID, Name: payload.Name, Arguments: args}})
		}
	}
	if len(payload.Usage) > 0 && string(payload.Usage) != "null" {
		emit(types.ActivityDelta{Usage: payload.Usage})
	}
}

type completionMessage struct {
	Content   string `json:"content"`
	ToolCalls []struct {
		Index    int    `json:"index"`
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls"`
}

func emitMessage(message completionMessage, emit func(types.ActivityDelta)) {
	if message.Content != "" {
		emit(types.ActivityDelta{Text: message.Content})
	}
	for _, call := range message.ToolCalls {
		emit(types.ActivityDelta{ToolCall: &types.ToolCall{
			Index: call.Index, ID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments,
		}})
	}
}
