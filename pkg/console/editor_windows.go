package console

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

const (
	eventKey = 0x0001

	vkBack   = 0x08
	vkTab    = 0x09
	vkReturn = 0x0D
	vkShift  = 0x10
	vkLShift = 0xA0
	vkRShift = 0xA1
	vkEscape = 0x1B
	vkEnd    = 0x23
	vkHome   = 0x24
	vkLeft   = 0x25
	vkUp     = 0x26
	vkRight  = 0x27
	vkDown   = 0x28
	vkDelete = 0x2E

	rightAltPressed  = 0x0001
	leftAltPressed   = 0x0002
	rightCtrlPressed = 0x0004
	leftCtrlPressed  = 0x0008
	shiftPressed     = 0x0010

	enableWindowInput = 0x0008
)

var (
	kernel32DLL                    = syscall.NewLazyDLL("kernel32.dll")
	user32DLL                      = syscall.NewLazyDLL("user32.dll")
	procGetConsoleMode             = kernel32DLL.NewProc("GetConsoleMode")
	procSetConsoleMode             = kernel32DLL.NewProc("SetConsoleMode")
	procReadConsoleInputW          = kernel32DLL.NewProc("ReadConsoleInputW")
	procGetNumberOfConsoleInputEvt = kernel32DLL.NewProc("GetNumberOfConsoleInputEvents")
	procGetAsyncKeyState           = user32DLL.NewProc("GetAsyncKeyState")
)

type winInputRecord struct {
	eventType uint16
	padding   uint16
	event     [16]byte
}

type winKeyEvent struct {
	keyDown         bool
	repeatCount     uint16
	virtualKeyCode  uint16
	virtualScanCode uint16
	unicodeChar     rune
	controlKeyState uint32
}

func parseWinKeyEvent(rec winInputRecord) winKeyEvent {
	bKeyDown := binary.LittleEndian.Uint32(rec.event[0:4])
	repeat := binary.LittleEndian.Uint16(rec.event[4:6])
	vk := binary.LittleEndian.Uint16(rec.event[6:8])
	scan := binary.LittleEndian.Uint16(rec.event[8:10])
	uch := binary.LittleEndian.Uint16(rec.event[10:12])
	ctrl := binary.LittleEndian.Uint32(rec.event[12:16])
	return winKeyEvent{
		keyDown:         bKeyDown != 0,
		repeatCount:     repeat,
		virtualKeyCode:  vk,
		virtualScanCode: scan,
		unicodeChar:     rune(uch),
		controlKeyState: ctrl,
	}
}

func isHardwareShiftDown() bool {
	for _, vk := range []uintptr{vkShift, vkLShift, vkRShift} {
		r, _, _ := procGetAsyncKeyState.Call(vk)
		if uint16(r)&0x8000 != 0 {
			return true
		}
	}
	return false
}

func isInteractiveStdinConsole() (syscall.Handle, uint32, bool) {
	if os.Stdin == nil {
		return 0, 0, false
	}
	h := syscall.Handle(os.Stdin.Fd())
	var mode uint32
	r1, _, _ := procGetConsoleMode.Call(uintptr(h), uintptr(unsafe.Pointer(&mode)))
	if r1 == 0 {
		return 0, 0, false
	}
	return h, mode, true
}

func pendingInputEventCount(h syscall.Handle) uint32 {
	var count uint32
	r1, _, _ := procGetNumberOfConsoleInputEvt.Call(uintptr(h), uintptr(unsafe.Pointer(&count)))
	if r1 == 0 {
		return 0
	}
	return count
}

func readSingleKeyEvent(h syscall.Handle) (winKeyEvent, bool, error) {
	var rec winInputRecord
	var read uint32
	r1, _, err := procReadConsoleInputW.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&rec)),
		1,
		uintptr(unsafe.Pointer(&read)),
	)
	if r1 == 0 || read == 0 {
		return winKeyEvent{}, false, err
	}
	if rec.eventType != eventKey {
		return winKeyEvent{}, false, nil
	}
	ke := parseWinKeyEvent(rec)
	if !ke.keyDown {
		return winKeyEvent{}, false, nil
	}
	return ke, true, nil
}

// ConsoleEditor provides an interactive multi-line SQL and command line editor
// supporting [Shift+Enter] newline continuation, [Tab] / [Shift+Tab] 4-space indentation,
// smart SQL block auto-indentation, multi-line clipboard paste, cursor movement, and command history.
type ConsoleEditor struct {
	fallbackReader *bufio.Reader
	history        []string
}

func NewConsoleEditor(r *bufio.Reader) *ConsoleEditor {
	return &ConsoleEditor{
		fallbackReader: r,
		history: []string{
			"SHOW TABLES;",
			"DESCRIBE users;",
			"DESCRIBE orders;",
			"SELECT u.shard_id, u.user_id, u.name, o.order_id, o.product_name, o.amount_usd, p.payment_method FROM users u INNER JOIN orders o ON u.user_id = o.user_id INNER JOIN payments p ON o.order_id = p.order_id ORDER BY o.amount_usd DESC LIMIT 6;",
			"SELECT shard_id, user_id, name, region, balance_usd, RANK() OVER (PARTITION BY region ORDER BY balance_cents DESC) AS regional_rank FROM users LIMIT 8;",
			"WITH high_value AS (SELECT * FROM users WHERE balance_cents >= 500000) SELECT region, COUNT(*) AS vip_users, ROUND(AVG(balance_usd), 2) AS avg_vip_usd, MAX(balance_usd) AS max_vip_usd FROM high_value GROUP BY region HAVING COUNT(*) >= 5 ORDER BY avg_vip_usd DESC;",
		},
	}
}

// ReadCommandOrSQL reads either a single-line command or a multi-line SQL statement.
// - Pressing [Shift+Enter] (or [Ctrl+Enter] / [Alt+Enter]) ALWAYS opens a new indented continuation line.
// - Pressing [Tab] inserts 4 spaces of indentation (aligned to 4-space tab stops); [Shift+Tab] un-indents 4 spaces.
// - Pressing normal [Enter] executes immediately for single-line commands/queries, or continues automatically
//   if a multi-line SQL block has unclosed parentheses '(' or an incomplete trailing SQL clause.
func (ce *ConsoleEditor) ReadCommandOrSQL(prompt string, continuationPrompt string, allowMultiLine bool) (string, error) {
	h, origMode, isConsole := isInteractiveStdinConsole()
	if !isConsole {
		return ce.readFromFallbackPipe(allowMultiLine)
	}

	// Switch console stdin to raw key-event mode so ReadConsoleInputW captures Shift+Enter, Tab, and Arrow keys
	_, _, _ = procSetConsoleMode.Call(uintptr(h), uintptr(enableWindowInput))
	defer func() {
		_, _, _ = procSetConsoleMode.Call(uintptr(h), uintptr(origMode))
	}()

	fmt.Print(prompt)

	var lines []string
	var cur []rune
	cursor := 0
	histIdx := len(ce.history)
	savedCurrent := ""

	redrawCurrentLine := func(activePrompt string) {
		lineStr := string(cur)
		// Clear from start of line and reprint activePrompt + current line
		fmt.Printf("\r\033[2K%s%s", activePrompt, lineStr)
		// Move cursor back if not at end of line
		if cursor < len(cur) {
			back := len(cur) - cursor
			fmt.Printf("\033[%dD", back)
		}
	}

	getActivePrompt := func() string {
		if len(lines) == 0 {
			return prompt
		}
		return continuationPrompt
	}

	startNewContinuationLine := func(autoIndent bool) {
		completedLine := string(cur)
		lines = append(lines, completedLine)
		fmt.Print("\r\n")

		indentSpaces := 0
		if autoIndent {
			indentSpaces = computeSmartNextIndent(lines)
		}
		cur = make([]rune, indentSpaces)
		for i := 0; i < indentSpaces; i++ {
			cur[i] = ' '
		}
		cursor = len(cur)
		fmt.Printf("%s%s", continuationPrompt, string(cur))
	}

	for {
		ke, ok, err := readSingleKeyEvent(h)
		if err != nil {
			return "", err
		}
		if !ok {
			continue
		}

		// Handle ESC or VT escape sequences (in case terminal emulator sends ANSI CSI sequences)
		if ke.virtualKeyCode == vkEscape || ke.unicodeChar == 0x1b {
			if pendingInputEventCount(h) > 0 {
				seq := readPendingEscapeSequence(h)
				switch seq {
				case "[A": // Up arrow
					if len(lines) == 0 && len(ce.history) > 0 && histIdx > 0 {
						if histIdx == len(ce.history) {
							savedCurrent = string(cur)
						}
						histIdx--
						cur = []rune(ce.history[histIdx])
						cursor = len(cur)
						redrawCurrentLine(getActivePrompt())
					}
					continue
				case "[B": // Down arrow
					if len(lines) == 0 && histIdx < len(ce.history) {
						histIdx++
						if histIdx == len(ce.history) {
							cur = []rune(savedCurrent)
						} else {
							cur = []rune(ce.history[histIdx])
						}
						cursor = len(cur)
						redrawCurrentLine(getActivePrompt())
					}
					continue
				case "[C": // Right arrow
					if cursor < len(cur) {
						cursor++
						fmt.Print("\033[C")
					}
					continue
				case "[D": // Left arrow
					if cursor > 0 {
						cursor--
						fmt.Print("\033[D")
					}
					continue
				case "[H", "[1~": // Home
					cursor = 0
					redrawCurrentLine(getActivePrompt())
					continue
				case "[F", "[4~": // End
					cursor = len(cur)
					redrawCurrentLine(getActivePrompt())
					continue
				case "[3~": // Delete
					if cursor < len(cur) {
						cur = append(cur[:cursor], cur[cursor+1:]...)
						redrawCurrentLine(getActivePrompt())
					}
					continue
				case "[Z": // Shift+Tab
					cur, cursor = unindentRuneLine(cur, cursor)
					redrawCurrentLine(getActivePrompt())
					continue
				case "[13;2u", "[27;2;13~", "\r", "\n": // Shift+Enter or Alt+Enter via CSI-u
					if allowMultiLine {
						startNewContinuationLine(true)
						continue
					}
				}
				continue
			}
			// Standalone Esc key: clear current line (or cancel multi-line buffer if current line is empty)
			if len(cur) > 0 {
				cur = nil
				cursor = 0
				redrawCurrentLine(getActivePrompt())
			} else if len(lines) > 0 {
				fmt.Print("\r\n  [Multi-line SQL cancelled]\r\n")
				return "", nil
			}
			continue
		}

		// Handle Ctrl+C (0x03)
		if ke.unicodeChar == 3 {
			if len(lines) > 0 || len(cur) > 0 {
				fmt.Print("^C\r\n")
				return "", nil
			}
			fmt.Print("^C\r\n")
			return "exit", nil
		}

		// Handle Ctrl+L (0x0C) -> clear screen
		if ke.unicodeChar == 12 {
			return "clear", nil
		}

		// Handle Arrow Keys, Home, End, Delete via VirtualKeyCode
		switch ke.virtualKeyCode {
		case vkLeft:
			if cursor > 0 {
				cursor--
				fmt.Print("\033[D")
			}
			continue
		case vkRight:
			if cursor < len(cur) {
				cursor++
				fmt.Print("\033[C")
			}
			continue
		case vkHome:
			cursor = 0
			redrawCurrentLine(getActivePrompt())
			continue
		case vkEnd:
			cursor = len(cur)
			redrawCurrentLine(getActivePrompt())
			continue
		case vkDelete:
			if cursor < len(cur) {
				cur = append(cur[:cursor], cur[cursor+1:]...)
				redrawCurrentLine(getActivePrompt())
			}
			continue
		case vkUp:
			if len(lines) == 0 && len(ce.history) > 0 && histIdx > 0 {
				if histIdx == len(ce.history) {
					savedCurrent = string(cur)
				}
				histIdx--
				cur = []rune(ce.history[histIdx])
				cursor = len(cur)
				redrawCurrentLine(getActivePrompt())
			}
			continue
		case vkDown:
			if len(lines) == 0 && histIdx < len(ce.history) {
				histIdx++
				if histIdx == len(ce.history) {
					cur = []rune(savedCurrent)
				} else {
					cur = []rune(ce.history[histIdx])
				}
				cursor = len(cur)
				redrawCurrentLine(getActivePrompt())
			}
			continue
		case vkBack:
			if cursor > 0 {
				// Smart 4-space unindent if Backspace is pressed inside leading indentation
				if isAllLeadingSpaces(cur[:cursor]) && cursor%4 == 0 {
					delCount := 4
					if cursor < 4 {
						delCount = cursor
					}
					cur = append(cur[:cursor-delCount], cur[cursor:]...)
					cursor -= delCount
				} else {
					cur = append(cur[:cursor-1], cur[cursor:]...)
					cursor--
				}
				redrawCurrentLine(getActivePrompt())
			} else if len(lines) > 0 {
				// Backspace at column 0 of a continuation line: merge back onto previous line
				prev := lines[len(lines)-1]
				lines = lines[:len(lines)-1]
				cur = []rune(prev)
				cursor = len(cur)
				fmt.Print("\r\033[2K\033[A")
				redrawCurrentLine(getActivePrompt())
			}
			continue
		case vkTab:
			shiftDown := (ke.controlKeyState&shiftPressed) != 0 || isHardwareShiftDown()
			if shiftDown {
				cur, cursor = unindentRuneLine(cur, cursor)
			} else {
				spacesToAdd := 4 - (cursor % 4)
				if spacesToAdd <= 0 {
					spacesToAdd = 4
				}
				pad := make([]rune, spacesToAdd)
				for i := range pad {
					pad[i] = ' '
				}
				cur = append(cur[:cursor], append(pad, cur[cursor:]...)...)
				cursor += spacesToAdd
			}
			redrawCurrentLine(getActivePrompt())
			continue
		}

		// Handle Enter / Return / Linefeed (Ctrl+J)
		if ke.virtualKeyCode == vkReturn || ke.unicodeChar == '\r' || ke.unicodeChar == '\n' {
			shiftOrMod := (ke.controlKeyState&(shiftPressed|leftAltPressed|rightAltPressed|leftCtrlPressed|rightCtrlPressed)) != 0 ||
				isHardwareShiftDown() ||
				(ke.unicodeChar == '\n' && ke.virtualKeyCode != vkReturn)

			isPastingMore := pendingInputEventCount(h) > 0

			if allowMultiLine && shiftOrMod {
				// Explicit [Shift+Enter] (or [Ctrl+Enter] / [Alt+Enter]) -> ALWAYS start a new indented line!
				startNewContinuationLine(true)
				continue
			}

			// If user typed trailing backslash '\' for explicit line continuation
			trimmedCur := strings.TrimRight(string(cur), " \t")
			if allowMultiLine && strings.HasSuffix(trimmedCur, `\`) && !strings.HasPrefix(strings.TrimSpace(trimmedCur), `\`) {
				cur = []rune(strings.TrimSuffix(trimmedCur, `\`))
				cursor = len(cur)
				redrawCurrentLine(getActivePrompt())
				startNewContinuationLine(true)
				continue
			}

			// If user pressed normal Enter on an empty continuation line, execute the accumulated buffer
			if len(lines) > 0 && strings.TrimSpace(string(cur)) == "" && !isPastingMore {
				fmt.Print("\r\n")
				full := strings.TrimSpace(strings.Join(lines, "\n"))
				ce.recordHistory(full)
				return full, nil
			}

			candidateLines := append(append([]string{}, lines...), string(cur))
			if allowMultiLine && (isPastingMore && needsMoreOnPaste(candidateLines) || needsMoreSQLInput(candidateLines)) {
				startNewContinuationLine(!isPastingMore)
				continue
			}

			fmt.Print("\r\n")
			full := strings.TrimSpace(strings.Join(candidateLines, "\n"))
			ce.recordHistory(full)
			return full, nil
		}

		// Printable Unicode character
		if ke.unicodeChar >= 32 {
			ch := ke.unicodeChar
			// Smart auto-unindent when typing ')' at the start of an indented continuation line
			if ch == ')' && len(lines) > 0 && isAllLeadingSpaces(cur[:cursor]) && cursor >= 4 {
				cur = append(cur[:cursor-4], cur[cursor:]...)
				cursor -= 4
			}
			if cursor == len(cur) {
				cur = append(cur, ch)
				cursor++
				if ch == ')' && len(lines) > 0 {
					redrawCurrentLine(getActivePrompt())
				} else {
					fmt.Printf("%c", ch)
				}
			} else {
				cur = append(cur[:cursor], append([]rune{ch}, cur[cursor:]...)...)
				cursor++
				redrawCurrentLine(getActivePrompt())
			}
		}
	}
}

func (ce *ConsoleEditor) recordHistory(entry string) {
	trimmed := strings.TrimSpace(entry)
	if trimmed == "" {
		return
	}
	// Collapse multi-line into single line for Up/Down arrow history recall, or preserve if short
	single := strings.Join(strings.Fields(trimmed), " ")
	if len(ce.history) > 0 && ce.history[len(ce.history)-1] == single {
		return
	}
	ce.history = append(ce.history, single)
	if len(ce.history) > 100 {
		ce.history = ce.history[len(ce.history)-100:]
	}
}

func (ce *ConsoleEditor) readFromFallbackPipe(allowMultiLine bool) (string, error) {
	var lines []string
	for {
		line, err := ce.fallbackReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}
		clean := strings.TrimRight(line, "\r\n")
		clean = strings.TrimPrefix(clean, "\xef\xbb\xbf")
		// Expand tabs to 4 spaces
		clean = strings.ReplaceAll(clean, "\t", "    ")

		// If we are in a multi-line SQL block and an empty line is encountered, execute accumulated buffer
		if len(lines) > 0 && strings.TrimSpace(clean) == "" {
			return strings.TrimSpace(strings.Join(lines, "\n")), nil
		}

		lines = append(lines, clean)

		if err == io.EOF {
			if len(lines) == 1 && strings.TrimSpace(lines[0]) == "" {
				return "", io.EOF
			}
			return strings.TrimSpace(strings.Join(lines, "\n")), nil
		}

		if !allowMultiLine || !needsMoreSQLInput(lines) {
			return strings.TrimSpace(strings.Join(lines, "\n")), nil
		}
	}
}

func readPendingEscapeSequence(h syscall.Handle) string {
	var b strings.Builder
	for i := 0; i < 8 && pendingInputEventCount(h) > 0; i++ {
		ke, ok, err := readSingleKeyEvent(h)
		if err != nil || !ok {
			break
		}
		if ke.unicodeChar > 0 {
			b.WriteRune(ke.unicodeChar)
			ch := ke.unicodeChar
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '~' || ch == '\r' || ch == '\n' {
				break
			}
		}
	}
	return b.String()
}

func isAllLeadingSpaces(runes []rune) bool {
	if len(runes) == 0 {
		return false
	}
	for _, r := range runes {
		if r != ' ' {
			return false
		}
	}
	return true
}

func unindentRuneLine(cur []rune, cursor int) ([]rune, int) {
	leading := 0
	for leading < len(cur) && cur[leading] == ' ' && leading < 4 {
		leading++
	}
	if leading == 0 {
		return cur, cursor
	}
	cur = cur[leading:]
	cursor -= leading
	if cursor < 0 {
		cursor = 0
	}
	return cur, cursor
}

// computeSmartNextIndent computes the indentation (in spaces) for the next line after [Shift+Enter] or SQL continuation.
func computeSmartNextIndent(lines []string) int {
	if len(lines) == 0 {
		return 0
	}
	prev := lines[len(lines)-1]
	leading := 0
	for leading < len(prev) && prev[leading] == ' ' {
		leading++
	}
	trimmed := strings.TrimSpace(prev)
	upper := strings.ToUpper(trimmed)

	// Increase indent by 4 spaces if the previous line opens a parenthesis '(' or begins a SQL clause block
	openParens := countUnquotedParensDelta(trimmed)
	if openParens > 0 ||
		strings.HasSuffix(trimmed, "(") ||
		upper == "SELECT" ||
		upper == "FROM" ||
		upper == "WHERE" ||
		upper == "VALUES" ||
		upper == "SET" ||
		upper == "GROUP BY" ||
		upper == "ORDER BY" ||
		upper == "HAVING" ||
		strings.HasSuffix(upper, " BEGIN") ||
		upper == "BEGIN" ||
		strings.HasSuffix(upper, " CASE") ||
		upper == "CASE" {
		return leading + 4
	}

	return leading
}

func countUnquotedParensDelta(s string) int {
	delta := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		} else if !inSingle && !inDouble {
			if ch == '(' {
				delta++
			} else if ch == ')' {
				delta--
			}
		}
	}
	return delta
}

func needsMoreOnPaste(lines []string) bool {
	joined := strings.TrimSpace(strings.Join(lines, "\n"))
	if joined == "" {
		return false
	}
	if !startsLikeSQL(joined) {
		return false
	}
	return !strings.HasSuffix(joined, ";") || hasUnclosedQuotesOrParens(joined)
}

// needsMoreSQLInput determines whether normal [Enter] should automatically continue onto a new line
// because the user is in the middle of typing an incomplete multi-line SQL statement.
func needsMoreSQLInput(lines []string) bool {
	joined := strings.TrimSpace(strings.Join(lines, "\n"))
	if joined == "" {
		return false
	}
	if !startsLikeSQL(joined) {
		return false
	}

	// 1. If quotes or parentheses are unclosed (e.g., CREATE TABLE foo ( ... ), continue automatically
	if hasUnclosedQuotesOrParens(joined) {
		return true
	}

	// 2. If statement already ends with ';', it is complete
	if strings.HasSuffix(joined, ";") {
		return false
	}

	// 3. If the user is ALREADY in multi-line SQL mode (len(lines) > 1), keep reading lines until ';'
	// (or until they press Enter on an empty line, which is handled before needsMoreSQLInput).
	if len(lines) > 1 {
		return true
	}

	// 4. Single line so far (len(lines) == 1) without trailing ';':
	lastLine := strings.TrimSpace(lines[0])
	if lastLine == "" {
		return false
	}
	if strings.HasSuffix(lastLine, ",") || strings.HasSuffix(lastLine, "(") {
		return true
	}

	upperLast := strings.ToUpper(lastLine)
	for _, dangling := range []string{
		"SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "OUTER",
		"ON", "AND", "OR", "SET", "VALUES", "INTO", "GROUP", "GROUP BY", "ORDER", "ORDER BY",
		"HAVING", "UNION", "UNION ALL", "INTERSECT", "EXCEPT", "AS", "CASE", "WHEN", "THEN",
		"ELSE", "LIMIT", "OFFSET", "BETWEEN", "IN", "EXISTS", "TABLE", "VIEW", "ADD", "COLUMN",
	} {
		if upperLast == dangling || strings.HasSuffix(upperLast, " "+dangling) || strings.HasSuffix(upperLast, "\t"+dangling) {
			return true
		}
	}

	// 5. If line 1 starts with SELECT but has no FROM and no '(' function call (e.g. "SELECT a, b"), continue
	if strings.HasPrefix(upperLast, "SELECT ") && !strings.Contains(upperLast, " FROM ") && !strings.Contains(upperLast, "(") {
		return true
	}

	return false
}

func startsLikeSQL(input string) bool {
	clean := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(input), "\xef\xbb\xbf"))
	for strings.HasPrefix(clean, "--") {
		if nl := strings.IndexByte(clean, '\n'); nl != -1 {
			clean = strings.TrimSpace(clean[nl+1:])
		} else {
			return true
		}
	}
	fields := strings.Fields(clean)
	if len(fields) == 0 {
		return false
	}
	first := strings.ToUpper(strings.Trim(fields[0], ";\xef\xbb\xbf"))
	switch first {
	case "SELECT", "WITH", "INSERT", "REPLACE", "UPDATE", "DELETE", "TRUNCATE",
		"CREATE", "ALTER", "DROP", "SHOW", "DESCRIBE", "DESC", "EXPLAIN",
		"PRAGMA", "VALUES", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
		"REBALANCE", "RUN", "ANALYZE", "VACUUM", "COPY", "IMPORT", "SET", "RESET", "DISCARD":
		return true
	}
	return false
}

func hasUnclosedQuotesOrParens(s string) bool {
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	parens := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if !inSingle && !inDouble {
			if ch == '-' && i+1 < len(s) && s[i+1] == '-' {
				inLineComment = true
				i++
				continue
			}
			if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
				inBlockComment = true
				i++
				continue
			}
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		} else if !inSingle && !inDouble {
			if ch == '(' {
				parens++
			} else if ch == ')' && parens > 0 {
				parens--
			}
		}
	}
	return inSingle || inDouble || inBlockComment || parens > 0
}
