"""Minimal RISON decoder — just enough for Uwazi Library URL state.

RISON is the encoding Uwazi uses for the `q=` URL parameter: a compact,
URL-safe shorthand for JSON. We need to parse it to lift `types` and
`filters` out of a Library URL the user is currently looking at.

Spec (subset we implement):
    !t   !f   !n         → true, false, null
    !( … )               → array
    ( key:val , … )      → object
    'literal'            → string (with !-escapes: !! → !, !' → ')
    bare identifiers     → identifier strings (e.g. `desc`, `creationDate`)
    -?[0-9]+ (.[0-9]+)?  → number

Not implemented: nested ! escapes outside strings, exponent form numbers,
URI-form RISON. None of those appear in Uwazi Library URLs.

Public surface: `loads(s) -> Any`.
"""

from __future__ import annotations

from typing import Any


class RisonError(ValueError):
    pass


def loads(s: str) -> Any:
    """Parse a RISON string. Raises RisonError on malformed input."""
    parser = _Parser(s)
    value = parser.value()
    parser.skip_ws()
    if parser.i < len(parser.s):
        raise RisonError(f"trailing garbage at {parser.i}: {parser.s[parser.i:parser.i+20]!r}")
    return value


class _Parser:
    __slots__ = ("s", "i")

    def __init__(self, s: str) -> None:
        self.s = s
        self.i = 0

    def skip_ws(self) -> None:
        # Uwazi never emits whitespace in URL state, but tolerate it for
        # hand-crafted strings (tests, CLI args).
        while self.i < len(self.s) and self.s[self.i].isspace():
            self.i += 1

    def value(self) -> Any:
        self.skip_ws()
        if self.i >= len(self.s):
            raise RisonError("unexpected end of input")
        c = self.s[self.i]
        if c == "!":
            return self._bang()
        if c == "(":
            return self._object()
        if c == "'":
            return self._string()
        return self._scalar()

    def _bang(self) -> Any:
        self.i += 1  # !
        if self.i >= len(self.s):
            raise RisonError("dangling !")
        c = self.s[self.i]
        if c == "t":
            self.i += 1
            return True
        if c == "f":
            self.i += 1
            return False
        if c == "n":
            self.i += 1
            return None
        if c == "(":
            return self._array()
        raise RisonError(f"bad ! at {self.i}: {c!r}")

    def _object(self) -> dict:
        self.i += 1  # (
        obj: dict[str, Any] = {}
        self.skip_ws()
        if self.i < len(self.s) and self.s[self.i] == ")":
            self.i += 1
            return obj
        while True:
            k = self._key()
            self.skip_ws()
            if self.i >= len(self.s) or self.s[self.i] != ":":
                raise RisonError(f"expected ':' after key at {self.i}")
            self.i += 1
            obj[k] = self.value()
            self.skip_ws()
            if self.i < len(self.s) and self.s[self.i] == ",":
                self.i += 1
                continue
            break
        if self.i >= len(self.s) or self.s[self.i] != ")":
            raise RisonError(f"expected ')' at {self.i}")
        self.i += 1
        return obj

    def _array(self) -> list:
        self.i += 1  # (  (the ! was consumed by _bang)
        arr: list[Any] = []
        self.skip_ws()
        if self.i < len(self.s) and self.s[self.i] == ")":
            self.i += 1
            return arr
        while True:
            arr.append(self.value())
            self.skip_ws()
            if self.i < len(self.s) and self.s[self.i] == ",":
                self.i += 1
                continue
            break
        if self.i >= len(self.s) or self.s[self.i] != ")":
            raise RisonError(f"expected ')' at {self.i}")
        self.i += 1
        return arr

    def _string(self) -> str:
        self.i += 1  # '
        out: list[str] = []
        while self.i < len(self.s) and self.s[self.i] != "'":
            c = self.s[self.i]
            if c == "!" and self.i + 1 < len(self.s):
                nxt = self.s[self.i + 1]
                if nxt == "'":
                    out.append("'")
                    self.i += 2
                    continue
                if nxt == "!":
                    out.append("!")
                    self.i += 2
                    continue
            out.append(c)
            self.i += 1
        if self.i >= len(self.s):
            raise RisonError("unterminated string")
        self.i += 1  # '
        return "".join(out)

    def _key(self) -> str:
        self.skip_ws()
        if self.i < len(self.s) and self.s[self.i] == "'":
            return self._string()
        start = self.i
        while self.i < len(self.s) and (
            self.s[self.i].isalnum() or self.s[self.i] in "_-."
        ):
            self.i += 1
        if start == self.i:
            raise RisonError(f"expected key at {start}")
        return self.s[start:self.i]

    def _scalar(self) -> Any:
        start = self.i
        # A scalar is a run of identifier-ish chars terminated by `)`, `,`, `:`, end.
        while self.i < len(self.s) and self.s[self.i] not in "),:":
            self.i += 1
        raw = self.s[start:self.i]
        if raw == "":
            raise RisonError(f"empty scalar at {start}")
        # Try number first.
        try:
            if "." in raw or "e" in raw or "E" in raw:
                return float(raw)
            return int(raw)
        except ValueError:
            return raw  # bare identifier
