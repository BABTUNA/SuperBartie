// Package llm answers a question from retrieved rows. With an Anthropic key it
// asks Claude; without one it returns the best-matching row verbatim, so the
// demo degrades to "here is the freshest matching record" instead of failing.
package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/BABTUNA/superbartie/internal/config"
)

type Client interface {
	// Answer uses only the contexts given; it must not invent facts.
	Answer(ctx context.Context, question string, contexts []string) (string, error)
	Model() string
}

func New(cfg config.Config) Client {
	if cfg.LLMAPIKey == "" {
		return extractive{}
	}
	return &claude{
		client: anthropic.NewClient(option.WithAPIKey(cfg.LLMAPIKey)),
		model:  cfg.LLMModel,
	}
}

const system = `You answer questions about a wildlife tracking database using ONLY the records provided.
Records are the current state of the data as of the moment of the question; the data changes in real time.
Answer in one or two plain sentences. Cite the record you used by its id (e.g. "observation 5512").
If the records do not contain the answer, say so in one sentence. Never guess.`

type claude struct {
	client anthropic.Client
	model  string
}

func (c *claude) Model() string { return c.model }

func (c *claude) Answer(ctx context.Context, question string, contexts []string) (string, error) {
	var b strings.Builder
	b.WriteString("Records:\n")
	for i, t := range contexts {
		fmt.Fprintf(&b, "%d. %s\n", i+1, t)
	}
	fmt.Fprintf(&b, "\nQuestion: %s", question)

	resp, err := c.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model: anthropic.Model(c.model),
		// thinking is always on for this model and counts against the limit
		// leave room for it so a short answer is never cut off before it starts
		MaxTokens:    4096,
		System:       []anthropic.TextBlockParam{{Text: system}},
		OutputConfig: anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffortLow},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(b.String())),
		},
	})
	if err != nil {
		return "", fmt.Errorf("claude: %w", err)
	}
	if resp.StopReason == anthropic.StopReasonRefusal {
		return "I can't answer that one.", nil
	}
	var out strings.Builder
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			out.WriteString(t.Text)
		}
	}
	if out.Len() == 0 {
		return "", fmt.Errorf("claude: empty response (stop_reason %s)", resp.StopReason)
	}
	return strings.TrimSpace(out.String()), nil
}

// extractive is the no-key fallback: the top record is the answer.
type extractive struct{}

func (extractive) Model() string { return "extractive (no LLM key configured)" }

func (extractive) Answer(_ context.Context, _ string, contexts []string) (string, error) {
	if len(contexts) == 0 {
		return "No matching records.", nil
	}
	return "Closest record: " + contexts[0], nil
}
