package connector

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/eleme/go-thrift/parser"
)

type Client struct {
	Name           string
	Transport      thrift.TTransport
	InputProtocol  thrift.TProtocol
	OutputProtocol thrift.TProtocol
	SeqId          int32
	Thrift         *parser.Thrift
	NameTypeMap    map[string]thrift.TType
}

var defultNameTypeMap = map[string]thrift.TType{
	"stop":   thrift.STOP,
	"void":   thrift.VOID,
	"bool":   thrift.BOOL,
	"byte":   thrift.BYTE,
	"double": thrift.DOUBLE,
	"i16":    thrift.I16,
	"i32":    thrift.I32,
	"i64":    thrift.I64,
	"string": thrift.STRING,
	"struct": thrift.STRUCT,
	"map":    thrift.MAP,
	"set":    thrift.SET,
	"list":   thrift.LIST,
	"utf8":   thrift.UTF8,
	"utf16":  thrift.UTF16,
}

func NewClient(name string, hostPort string, timeout time.Duration, parsedThrift *parser.Thrift) (*Client, error) {
	nameTypeMap := defultNameTypeMap
	for name, _ := range parsedThrift.Enums {
		nameTypeMap[name] = thrift.I32
	}
	for name, _ := range parsedThrift.Structs {
		nameTypeMap[name] = thrift.STRUCT
	}
	for name, _ := range parsedThrift.Exceptions {
		nameTypeMap[name] = thrift.STRUCT
	}
	for name, thriftType := range parsedThrift.Typedefs {
		var ok bool
		if nameTypeMap[name], ok = nameTypeMap[thriftType.Name]; !ok {
			return nil, fmt.Errorf("type %s not found", thriftType.Name)
		}
	}

	transport, err := thrift.NewTSocketTimeout(hostPort, timeout)
	if err != nil {
		return nil, err
	}
	if err = transport.Open(); err != nil {
		return nil, err
	}
	factory := thrift.NewTBinaryProtocolFactoryDefault()
	bufferFactory := thrift.NewTBufferedTransportFactory(8192)
	return &Client{
		Transport:      transport,
		InputProtocol:  factory.GetProtocol(bufferFactory.GetTransport(transport)),
		OutputProtocol: factory.GetProtocol(bufferFactory.GetTransport(transport)),
		SeqId:          0,
		Thrift:         parsedThrift,
		Name:           name,
		NameTypeMap:    nameTypeMap,
	}, nil
}

func (client Client) lookupType(name string) thrift.TType {
	if ttype, ok := client.NameTypeMap[name]; ok {
		return ttype
	}
	return thrift.STOP
}

func (client Client) Call(method string, args ...interface{}) (response interface{}, err error) {
	if err = client.send(method, args...); err != nil {
		return
	}
	return client.recv()
}

func (client *Client) send(method string, args ...interface{}) (err error) {
	client.SeqId++
	if err = client.OutputProtocol.WriteMessageBegin(method, thrift.CALL, client.SeqId); err != nil {
		return err
	}
	if err = client.WriteRequest(method, args...); err != nil {
		return err
	}
	if err = client.OutputProtocol.WriteMessageEnd(); err != nil {
		return err
	}
	return client.OutputProtocol.Flush()
}

func (client *Client) recv() (response interface{}, err error) {
	methodName, messageTypeId, seqId, err := client.InputProtocol.ReadMessageBegin()
	if err != nil {
		return nil, err
	}
	if messageTypeId == thrift.EXCEPTION {
		exception := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		if exception, err = exception.Read(client.InputProtocol); err != nil {
			return nil, err
		}
		if err = client.InputProtocol.ReadMessageEnd(); err != nil {
			return nil, err
		}
		return nil, exception
	}
	if client.SeqId != seqId {
		return nil, thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "Response out of sequence")
	}
	if response, err = client.ReadResponse(methodName); err != nil {
		return nil, err
	}
	if err = client.InputProtocol.ReadMessageEnd(); err != nil {
		return nil, err
	}
	return response, nil
}

func (client Client) ReadResponse(methodName string) (response interface{}, err error) {
	var service *parser.Service
	var ok bool
	if service, ok = client.Thrift.Services[client.Name]; !ok {
		return nil, fmt.Errorf("service %s not exits.", client.Name)
	}
	var method *parser.Method
	if method, ok = service.Methods[methodName]; !ok {
		return nil, fmt.Errorf("method %s.%s not exits.", client.Name, methodName)
	}

	if _, err = client.InputProtocol.ReadStructBegin(); err != nil {
		return nil, err
	}
	var index int16
	if _, _, index, err = client.InputProtocol.ReadFieldBegin(); err != nil {
		return nil, err
	}
	var fieldType *parser.Type
	if index == 0 {
		fieldType = method.ReturnType
	} else if int(index) <= len(method.Exceptions) {
		fieldType = method.Exceptions[index-1].Type
	} else {
		return nil, fmt.Errorf("method %s index %s not exits.", methodName, index)
	}
	if response, err = client.ReadValue(fieldType); err != nil {
		return nil, err
	}
	if err = client.InputProtocol.ReadFieldEnd(); err != nil {
		return nil, err
	}
	if _, _, _, err = client.InputProtocol.ReadFieldBegin(); err != nil {
		return nil, err
	}
	if err = client.InputProtocol.ReadStructEnd(); err != nil {
		return nil, err
	}
	return response, nil
}

func (client Client) ReadValue(parserType *parser.Type) (response interface{}, err error) {
	switch client.lookupType(parserType.Name) {
	case thrift.BOOL:
		return client.InputProtocol.ReadBool()
	case thrift.BYTE:
		return client.InputProtocol.ReadByte()
	case thrift.DOUBLE:
		return client.InputProtocol.ReadDouble()
	case thrift.I16:
		return client.InputProtocol.ReadI16()
	case thrift.I32:
		return client.InputProtocol.ReadI32()
	case thrift.I64:
		return client.InputProtocol.ReadI64()
	case thrift.STRING:
		return client.InputProtocol.ReadString()
	case thrift.STRUCT:
		return client.ReadStruct(parserType)
	case thrift.MAP:
		return client.ReadMap(parserType)
	case thrift.LIST:
		return client.ReadList(parserType)
	case thrift.STOP:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", parserType)
	}
	return nil, fmt.Errorf("unsupported type %s", parserType)
}

func (client Client) ReadStruct(parserType *parser.Type) (interface{}, error) {
	var thriftStruct *parser.Struct
	var ok bool
	if thriftStruct, ok = client.Thrift.Structs[parserType.Name]; !ok {
		if thriftStruct, ok = client.Thrift.Exceptions[parserType.Name]; !ok {
			return nil, fmt.Errorf("struct %s not found.", parserType.Name)
		}
	}
	result := make(map[string]interface{})
	var err error

	if _, err = client.InputProtocol.ReadStructBegin(); err != nil {
	}
	for {
		var index int16
		var thriftType thrift.TType
		if _, thriftType, index, err = client.InputProtocol.ReadFieldBegin(); err != nil {
			return nil, err
		}

		if thriftType == thrift.STOP {
			break
		}
		if int(index) > len(thriftStruct.Fields) {
			return nil, fmt.Errorf("struct %s index %s not exists.", thriftStruct.Name, index)
		}
		field := thriftStruct.Fields[index-1]
		var value interface{}
		if value, err = client.ReadValue(field.Type); err != nil {
			return nil, err
		}
		result[field.Name] = value

		if err = client.InputProtocol.ReadFieldEnd(); err != nil {
			return nil, err
		}
	}
	if err = client.InputProtocol.ReadStructEnd(); err != nil {
		return nil, err
	}
	return result, nil
}

func (client Client) ReadMap(parserType *parser.Type) (interface{}, error) {
	result := make(map[string]interface{})
	var size int
	var err error
	if _, _, size, err = client.InputProtocol.ReadMapBegin(); err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		var key string
		var value interface{}
		if key, err = client.InputProtocol.ReadString(); err != nil {
			return nil, err
		}
		if value, err = client.ReadValue(parserType.ValueType); err != nil {
			return nil, err
		}
		result[key] = value
	}
	if err = client.InputProtocol.ReadMapEnd(); err != nil {
		return nil, err
	}
	return result, nil
}

func (client Client) ReadList(parserType *parser.Type) (interface{}, error) {
	result := make([]interface{}, 0)
	var size int
	var err error
	if _, size, err = client.InputProtocol.ReadListBegin(); err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		var value interface{}
		if value, err = client.ReadValue(parserType.ValueType); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err = client.InputProtocol.ReadListEnd(); err != nil {
		return nil, err
	}
	return result, nil
}

func (client Client) WriteRequest(methodName string, args ...interface{}) (err error) {
	var ok bool
	var service *parser.Service
	if service, ok = client.Thrift.Services[client.Name]; !ok {
		return fmt.Errorf("service %s not exits.", client.Name)
	}
	var method *parser.Method
	if method, ok = service.Methods[methodName]; !ok {
		return fmt.Errorf("method %s.%s not exits.", client.Name, methodName)
	}
	if err = client.OutputProtocol.WriteStructBegin(service.Name + method.Name + "Args"); err != nil {
		return err
	}

	for _, argument := range method.Arguments {
		if argument.ID > len(args) {
			return fmt.Errorf("No.%d arg %s not exits.", argument.ID, argument.Name)
		}
		arg := args[argument.ID-1]

		if err = client.WriteField(argument, arg); err != nil {
			return err
		}
	}

	if err = client.OutputProtocol.WriteFieldStop(); err != nil {
		return err
	}

	if err = client.OutputProtocol.WriteStructEnd(); err != nil {
		return err
	}
	return nil
}

func (client Client) WriteStruct(parserType *parser.Type, value interface{}) (err error) {
	var thriftStruct *parser.Struct
	var ok bool
	if thriftStruct, ok = client.Thrift.Structs[parserType.Name]; !ok {
		if thriftStruct, ok = client.Thrift.Exceptions[parserType.Name]; !ok {
			return fmt.Errorf("struct %s not found.", parserType.Name)
		}
	}
	var mapValue map[string]interface{}
	if mapValue, ok = value.(map[string]interface{}); !ok {
		return fmt.Errorf("%v type assertion to map[string]interface{} failed.", value)
	}
	if err = client.OutputProtocol.WriteStructBegin(thriftStruct.Name); err != nil {
		return err
	}
	for _, field := range thriftStruct.Fields {
		fieldValue, ok := mapValue[field.Name]
		if ok {
			if err = client.WriteField(field, fieldValue); err != nil {
				return err
			}
		} else {
			if !field.Optional {
				return fmt.Errorf("field %s required.", field.Name)
			}
		}
	}
	if err = client.OutputProtocol.WriteFieldStop(); err != nil {
		return err
	}
	if err = client.OutputProtocol.WriteStructEnd(); err != nil {
		return err
	}
	return nil
}

func (client Client) WriteMap(parserType *parser.Type, value interface{}) (err error) {
	var mapValue map[string]interface{}
	var ok bool
	if mapValue, ok = value.(map[string]interface{}); !ok {
		return fmt.Errorf("%v type assertion to map[string]interface{} failed.", value)
	}
	valueType := parserType.ValueType
	if err = client.OutputProtocol.WriteMapBegin(thrift.STRING, client.lookupType(valueType.Name), len(mapValue)); err != nil {
		return err
	}
	for k, v := range mapValue {
		if err = client.OutputProtocol.WriteString(k); err != nil {
			return err
		}
		if err = client.WriteValue(valueType, v); err != nil {
			return err
		}
	}
	if err = client.OutputProtocol.WriteMapEnd(); err != nil {
		return err
	}
	return nil
}

func (client Client) WriteList(parserType *parser.Type, value interface{}) (err error) {
	var listValue []interface{}
	var ok bool
	if listValue, ok = value.([]interface{}); !ok {
		return fmt.Errorf("%v type assertion to []interface{} failed.", value)
	}
	valueType := parserType.ValueType
	if err = client.OutputProtocol.WriteListBegin(client.lookupType(valueType.Name), len(listValue)); err != nil {
		return err
	}
	for _, v := range listValue {
		if err = client.WriteValue(valueType, v); err != nil {
			return err
		}
	}
	if err = client.OutputProtocol.WriteListEnd(); err != nil {
		return err
	}
	return nil
}

func (client Client) WriteField(argument *parser.Field, arg interface{}) (err error) {
	if err := client.OutputProtocol.WriteFieldBegin(argument.Name, client.lookupType(argument.Type.Name), int16(argument.ID)); err != nil {
		return err
	}
	if err := client.WriteValue(argument.Type, arg); err != nil {
		return err
	}
	if err := client.OutputProtocol.WriteFieldEnd(); err != nil {
		return err
	}
	return nil
}

func (client Client) WriteValue(parserType *parser.Type, value interface{}) (err error) {
	switch client.lookupType(parserType.Name) {
	case thrift.BOOL:
		if boolValue, ok := value.(bool); ok {
			return client.OutputProtocol.WriteBool(boolValue)
		}
	case thrift.BYTE:
		if byteValue, ok := value.(byte); ok {
			return client.OutputProtocol.WriteByte(byteValue)
		}
	case thrift.DOUBLE:
		if doubleValue, ok := Float64(value); ok {
			return client.OutputProtocol.WriteDouble(doubleValue)
		}
	case thrift.I16:
		if i16Value, ok := Int16(value); ok {
			return client.OutputProtocol.WriteI16(i16Value)
		}
	case thrift.I32:
		if i32Value, ok := Int32(value); ok {
			return client.OutputProtocol.WriteI32(i32Value)
		}
	case thrift.I64:
		if i64Value, ok := Int64(value); ok {
			return client.OutputProtocol.WriteI64(i64Value)
		}
	case thrift.STRING:
		if stringValue, ok := value.(string); ok {
			return client.OutputProtocol.WriteString(stringValue)
		}
	case thrift.STRUCT:
		return client.WriteStruct(parserType, value)
	case thrift.MAP:
		return client.WriteMap(parserType, value)
	case thrift.LIST:
		return client.WriteList(parserType, value)
	default:
		return fmt.Errorf("unsupported type %s", parserType)
	}
	return fmt.Errorf("cannot convert %v to type %s.", value, parserType.Name)
}

func Int16(value interface{}) (int16, bool) {
	switch value.(type) {
	case float32, float64:
		return int16(reflect.ValueOf(value).Float()), true
	case int, int8, int16, int32, int64:
		return int16(reflect.ValueOf(value).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return int16(reflect.ValueOf(value).Uint()), true
	case string:
		int64Value, err := strconv.ParseInt(reflect.ValueOf(value).String(), 10, 0)
		if err != nil {
			return 0, false
		}
		return int16(int64Value), true
	}
	return 0, false
}

func Int32(value interface{}) (int32, bool) {
	switch value.(type) {
	case float32, float64:
		return int32(reflect.ValueOf(value).Float()), true
	case int, int8, int16, int32, int64:
		return int32(reflect.ValueOf(value).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return int32(reflect.ValueOf(value).Uint()), true
	case string:
		int64Value, err := strconv.ParseInt(reflect.ValueOf(value).String(), 10, 0)
		if err != nil {
			return 0, false
		}
		return int32(int64Value), true
	}
	return 0, false
}

func Int64(value interface{}) (int64, bool) {
	switch value.(type) {
	case float32, float64:
		return int64(reflect.ValueOf(value).Float()), true
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(value).Int(), true
	case uint, uint8, uint16, uint32, uint64:
		return int64(reflect.ValueOf(value).Uint()), true
	case string:
		int64Value, err := strconv.ParseInt(reflect.ValueOf(value).String(), 10, 0)
		if err != nil {
			return 0, false
		}
		return int64Value, true
	}
	return 0, false
}

func Float64(value interface{}) (float64, bool) {
	switch value.(type) {
	case float32, float64:
		return reflect.ValueOf(value).Float(), true
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(value).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(value).Uint()), true
	case string:
		float64Value, err := strconv.ParseFloat(reflect.ValueOf(value).String(), 64)
		if err != nil {
			return 0, false
		}
		return float64Value, true
	}
	return 0, false
}
