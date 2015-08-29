package connector

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/eleme/go-thrift/parser"
)

type Service struct {
	*parser.Service
	Structs map[string]*parser.Struct
	Types   map[string]thrift.TType
}

func NewService(parsedThrift *parser.Thrift, name string) (*Service, error) {
	var ok bool
	var service *parser.Service
	if service, ok = parsedThrift.Services[name]; !ok {
		return nil, fmt.Errorf("service %s not exits.", name)
	}

	var structs map[string]*parser.Struct
	structs = make(map[string]*parser.Struct)
	for name, thriftException := range parsedThrift.Exceptions {
		structs[name] = thriftException
	}
	for name, thriftStruct := range parsedThrift.Structs {
		structs[name] = thriftStruct
	}

	var types map[string]thrift.TType
	types = defultTypeNameMap
	for name, _ := range parsedThrift.Enums {
		types[name] = thrift.I32
	}
	for name, _ := range parsedThrift.Structs {
		types[name] = thrift.STRUCT
	}
	for name, _ := range parsedThrift.Exceptions {
		types[name] = thrift.STRUCT
	}
	for name, thriftType := range parsedThrift.Typedefs {
		var ok bool
		if types[name], ok = types[thriftType.Name]; !ok {
			return nil, fmt.Errorf("type %s not found", thriftType.Name)
		}
	}

	return &Service{service, structs, types}, nil
}

var defultTypeNameMap = map[string]thrift.TType{
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

func (service Service) LookupType(name string) thrift.TType {
	if ttype, ok := service.Types[name]; ok {
		return ttype
	}
	return thrift.STOP
}
