package raft


import "encoding/gob"
import "io"
import "reflect"
import "fmt"
import "sync"
import "unicode"
import "unicode/utf8"

var mu sync.Mutex
var errorCount int // for TestCapital
var checked map[reflect.Type]bool

// LabEncoder 是一个封装了 gob 编码器的结构体。
// 提供了对 gob 编码功能的封装。
type LabEncoder struct {
    gob *gob.Encoder // 内部使用的 gob 编码器
}

// NewEncoder 创建一个新的 LabEncoder 实例。
// 参数:
//   - w: 实现了 io.Writer 接口的写入器，用于写入编码后的数据。
// 返回:
//   - 返回一个新的 LabEncoder 实例。
func NewEncoder(w io.Writer) *LabEncoder {
    enc := &LabEncoder{}
    enc.gob = gob.NewEncoder(w) // 初始化内部的 gob 编码器
    return enc
}

// Encode 对给定的值进行编码。
// 参数:
//   - e: 要编码的值，必须是可编码的。
// 返回:
//   - 如果编码成功，返回 nil；否则返回错误。
func (enc *LabEncoder) Encode(e interface{}) error {
    checkValue(e) // 检查值是否有效
    return enc.gob.Encode(e) // 调用内部的 gob 编码器进行编码
}

// EncodeValue 对给定的 reflect.Value 进行编码。
// 参数:
//   - value: 要编码的 reflect.Value 对象。
// 返回:
//   - 如果编码成功，返回 nil；否则返回错误。
func (enc *LabEncoder) EncodeValue(value reflect.Value) error {
    checkValue(value.Interface()) // 检查值是否有效
    return enc.gob.EncodeValue(value) // 调用内部的 gob 编码器进行编码
}

// LabDecoder 是一个封装了 gob 解码器的结构体。
// 提供了对 gob 解码功能的封装。
type LabDecoder struct {
    gob *gob.Decoder // 内部使用的 gob 解码器
}

// NewDecoder 创建一个新的 LabDecoder 实例。
// 参数:
//   - r: 实现了 io.Reader 接口的读取器，用于读取编码后的数据。
// 返回:
//   - 返回一个新的 LabDecoder 实例。
func NewDecoder(r io.Reader) *LabDecoder {
    dec := &LabDecoder{}
    dec.gob = gob.NewDecoder(r) // 初始化内部的 gob 解码器
    return dec
}

// Decode 从输入流中解码数据到给定的值。
// 参数:
//   - e: 解码后的值，将被填充到这个变量中。
// 返回:
//   - 如果解码成功，返回 nil；否则返回错误。
func (dec *LabDecoder) Decode(e interface{}) error {
    checkValue(e) // 检查值是否有效
    checkDefault(e) // 检查默认值
    return dec.gob.Decode(e) // 调用内部的 gob 解码器进行解码
}

// Register 注册一个类型以便 gob 编码和解码。
// 参数:
//   - value: 要注册的类型。
// 返回:
//   - 无
func Register(value interface{}) {
    checkValue(value) // 检查值是否有效
    gob.Register(value) // 注册类型
}

// RegisterName 使用指定的名称注册一个类型以便 gob 编码和解码。
// 参数:
//   - name: 注册类型的名称。
//   - value: 要注册的类型。
// 返回:
//   - 无
func RegisterName(name string, value interface{}) {
    checkValue(value) // 检查值是否有效
    gob.RegisterName(name, value) // 使用指定的名称注册类型
}

// checkValue 检查给定的值是否有效。
// 参数:
//   - value: 要检查的值。
// 返回:
//   - 无
func checkValue(value interface{}) {
    checkType(reflect.TypeOf(value)) // 检查值的类型是否有效
}

// checkType 检查类型是否有效（假设 checkType 的实现是有效的）。
// 参数:
//   - t: 要检查的类型。
// 返回:
//   - 无
func checkType(t reflect.Type) {
	k := t.Kind() // 获取类型的种类（例如：结构体、切片、数组、指针、映射等）

	mu.Lock()
	// 只抱怨一次，并避免递归调用。
	if checked == nil {
		checked = map[reflect.Type]bool{} // 初始化检查过的类型映射
	}
	if checked[t] {
		mu.Unlock()
		return // 如果该类型已经检查过，则直接返回
	}
	checked[t] = true // 标记该类型已经检查过
	mu.Unlock()

	switch k {
	case reflect.Struct:
		// 如果类型是结构体，则检查结构体的每个字段
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i) // 获取字段
			rune, _ := utf8.DecodeRuneInString(f.Name) // 解码字段名中的第一个 rune
			if unicode.IsUpper(rune) == false {
				// 如果字段名的第一个字符不是大写字母，打印警告信息
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1 // 错误计数增加
				mu.Unlock()
			}
			checkType(f.Type) // 递归检查字段的类型
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		// 如果类型是切片、数组或指针，则递归检查元素的类型
		checkType(t.Elem())
		return
	case reflect.Map:
		// 如果类型是映射，则递归检查键和值的类型
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		// 对于其他类型，不做额外检查
		return
	}
}

//
// warn if the value contains non-default values,
// as it would if one sent an RPC but the reply
// struct was already modified. if the RPC reply
// contains default values, GOB won't overwrite
// the non-default value.
//
func checkDefault(value interface{}) {
	if value == nil {
		return
	}
	checkDefault1(reflect.ValueOf(value), 1, "")
}

func checkDefault1(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return // 如果递归深度超过 3 层，则停止递归
	}

	t := value.Type() // 获取 value 的类型
	k := t.Kind()     // 获取类型的种类（例如：结构体、指针、基本类型等）

	switch k {
	case reflect.Struct:
		// 如果类型是结构体，则检查每个字段
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)                 // 获取字段的值
			name1 := t.Field(i).Name             // 获取字段的名称
			if name != "" {
				name1 = name + "." + name1 // 生成完整的字段名称
			}
			checkDefault1(vv, depth+1, name1) // 递归检查字段
		}
		return
	case reflect.Ptr:
		// 如果类型是指针，且指针值为 nil，则直接返回
		if value.IsNil() {
			return
		}
		checkDefault1(value.Elem(), depth+1, name) // 递归检查指针指向的值
		return
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		// 如果类型是基本类型或字符串，则检查是否是默认值
		if reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) == false {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name() // 如果名称为空，则使用类型名称
				}
				// 警告信息：通常在 RPC 回复中重用相同的变量，或者在恢复持久化状态时使用已经有非默认值的变量会出现此警告
				fmt.Printf("labgob warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1 // 增加错误计数
			mu.Unlock()
		}
		return
	}
}
