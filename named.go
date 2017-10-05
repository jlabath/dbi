package dbi

import (
	"bytes"
	"io"
	"unicode"
)

func produceQuery(prefix rune, ph placeHolderFunc, inputQuery string) (string, []string, error) {
	ctx := parseContext{
		prefix: prefix,
		ph:     ph,
		in:     bytes.NewBufferString(inputQuery),
		out:    new(bytes.Buffer),
		argBuf: new(bytes.Buffer),
	}
	for fn := basicFn; fn != nil; {
		fn = fn(&ctx)
	}
	return ctx.out.String(), ctx.args, ctx.err
}

type parseContext struct {
	prefix rune
	ph     placeHolderFunc
	in     *bytes.Buffer
	out    *bytes.Buffer
	argBuf *bytes.Buffer
	args   []string
	err    error
}

type stateFn func(*parseContext) stateFn

func basicFn(pc *parseContext) stateFn {
	r, _, err := pc.in.ReadRune()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		pc.err = err
		return nil
	}

	if r == pc.prefix {
		return inArgFn
	}
	pc.out.WriteRune(r)
	return basicFn
}

func inArgFn(pc *parseContext) stateFn {
	r, _, err := pc.in.ReadRune()
	if err == io.EOF {
		return inArgEOFFn
	}
	if err != nil {
		pc.err = err
		return nil
	}

	if isEndOfArgumentRune(r) {
		//write place holder
		pc.out.WriteString(pc.ph())
		//write whatever rune this is to query buffer
		pc.out.WriteRune(r)
		//append the arg
		pc.args = append(pc.args, pc.argBuf.String())
		pc.argBuf.Reset()
		//back to basic state
		return basicFn
	}
	pc.argBuf.WriteRune(r)
	return inArgFn
}

func inArgEOFFn(pc *parseContext) stateFn {
	//write place holder
	pc.out.WriteString(pc.ph())
	//append the arg
	pc.args = append(pc.args, pc.argBuf.String())
	//back to basic state
	return nil
}

func isEndOfArgumentRune(r rune) bool {
	if unicode.IsLetter(r) {
		return false
	}
	if unicode.IsDigit(r) {
		return false
	}
	switch r {
	case '_':
		return false
	case '-':
		return false
	default:
		return true
	}
}
