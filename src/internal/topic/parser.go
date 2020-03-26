package topic

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const identPatternString = `[\-_0-9a-zA-Z]+`

var identPattern = regexp.MustCompile(identPatternString)

// Parser for the topic string.  It not only supports basic wildcards like
// single wildcards `+` and multilevel wildcards `#`, but also the URL-like
// query string for additional options of subscription.
//
// Grammar:
//
// ```antlr
// grammar topic;
//
// IDENT : [\-_0-9a-zA-Z]+ ;
//
// topic : (channel | '#') query? EOF
//       ;
//
// channel : IDENT ('/' channel | '/' '#')?
// 		   | '+' ('/' channel)?
//         ;
//
// query : '?' query_kv
//       ;
//
// query_kv : IDENT ('=' IDENT)? ('&' query_kv)?
//          ;
// ```
type Parser struct {
	srcTxt  string
	pos     int
	parts   []part
	options []*option
}

// New creates a new topic parser.
func New(srcTxt string) *Parser {
	return &Parser{
		srcTxt: srcTxt,
	}
}

// Parse the topic string to get a `Topic`.
func (p *Parser) Parse() (*Topic, error) {
	if err := p.scan(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Parser) scan() error {
	texts := strings.Split(p.srcTxt, "?")
	textsLen := len(texts)

	if textsLen > 2 {
		return errors.New("Too many '?' in topic string")
	}

	if err := p.scanParts(texts[0]); err != nil {
		return err
	}

	if textsLen == 1 {
		return nil
	}

	return p.scanOptions(texts[1])
}

func (p *Parser) scanParts(partsTxt string) error {
	parts := strings.Split(partsTxt, "/")

	for _, part := range parts {
		switch part {
		case "+":
			p.parts = append(p.parts, partSingleWildcard{})
		case "#":
			p.parts = append(p.parts, partMultiWildcard{})
		default:
			if !identPattern.Match([]byte(part)) {
				return errors.Errorf("Invalid identifier '%v'", part)
			}
			p.parts = append(p.parts, partName{value: part})
		}
	}

	return nil
}

func (p *Parser) scanOptions(optsTxt string) error {
	opts := strings.Split(optsTxt, "&")

	for _, opt := range opts {
		kv := strings.Split(opt, "=")
		kvLen := len(kv)

		if kvLen > 2 {
			return errors.Errorf("Too many '=' in '%v'", opt)
		}

		for _, v := range kv {
			if !identPattern.Match([]byte(v)) {
				return errors.Errorf("Invalid character(s) in '%v'", v)
			}
		}

		key := kv[0]
		var value string
		if kvLen == 2 {
			value = kv[1]
		}

		p.options = append(p.options, &option{Key: key, Value: value})
	}

	return nil
}
