package topic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewParser(t *testing.T) {
	assertion := assert.New(t)
	parser := NewParser("hello")
	assertion.NotNil(parser)
}

func TestParseOK(t *testing.T) {
	assertion := assert.New(t)
	topics := []string{
		"#",
		"a/b/c",
		"+/+",
		"a/+/c",
		"a",
		"a/#",
		"a/b/c?a=a",
		"a/b/c?a=a&b=b",
		"a/b/c?a&b",
	}
	for _, s := range topics {
		parser := NewParser(s)
		_, err := parser.Parse()
		assertion.NoError(err, s)
	}
}

func TestParseFail(t *testing.T) {
	assertion := assert.New(t)
	topics := []string{
		"",
		"/",
		"#/#",
		"#/+",
		"#/a",
		"+/#",
		"a/#/c",
		"/+/b",
		"a/+/",
		"a?",
		"a?=a",
		"?",
		"?a",
		"a?a=",
		"a?a&b=",
		"a?a&=b",
		"a?b?c",
	}
	for _, s := range topics {
		parser := NewParser(s)
		_, err := parser.Parse()
		assertion.Error(err, s)
	}
}
