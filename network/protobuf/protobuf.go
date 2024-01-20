package protobuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/name5566/leaf/chanrpc"
	"github.com/name5566/leaf/log"
	"github.com/yudeguang/ratelimit"
	"math"
	"reflect"
	"time"
)

var GRule = CreateRuleLimit(5)

func CreateRuleLimit(count int) *ratelimit.Rule {
	r := ratelimit.NewRule()
	//1秒访问五次
	r.AddRule(time.Second, count)
	return r
}

// -------------------------
// | id | protobuf message |
// -------------------------
type Processor struct {
	littleEndian bool
	//msgInfo      []*MsgInfo
	msgInfo map[uint16]*MsgInfo
	msgID   map[reflect.Type]uint16
}

type MsgInfo struct {
	msgType       reflect.Type
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

type MsgHandler func([]interface{})

type MsgRaw struct {
	msgID      uint16
	msgRawData []byte
}

func NewProcessor() *Processor {
	p := new(Processor)
	p.littleEndian = false
	p.msgID = make(map[reflect.Type]uint16)

	p.msgInfo = make(map[uint16]*MsgInfo)

	return p
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) Register(msg proto.Message) uint16 {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("protobuf message pointer required")
	}
	if _, ok := p.msgID[msgType]; ok {
		log.Fatal("message %s is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
	}

	i := new(MsgInfo)
	i.msgType = msgType
	//p.msgInfo = append(p.msgInfo, i)
	id := uint16(len(p.msgInfo))

	p.msgInfo[id] = i

	p.msgID[msgType] = id
	return id
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRouter(msg proto.Message, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("SetRouter--message %s not registered", msgType)
	}

	p.msgInfo[id].msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetHandler(msg proto.Message, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("SetHandler--message %s not registered", msgType)
	}

	p.msgInfo[id].msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRawHandler(id uint16, msgRawHandler MsgHandler) {
	if id >= uint16(len(p.msgInfo)) {
		log.Fatal("SetRawHandler--message id %v not registered", id)
	}

	p.msgInfo[id].msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *Processor) Route(msg interface{}, userData interface{}) error {

	//fmt.Println(1111111)
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		// if msgRaw.msgID >= uint16(len(p.msgInfo)) {
		// 	return fmt.Errorf("Route--1--message id %v not registered", msgRaw.msgID)
		// }
		i := p.msgInfo[msgRaw.msgID]
		if i.msgRawHandler != nil {
			i.msgRawHandler([]interface{}{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// protobuf
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	//调试信息，正式屏蔽 start
	PrintRouteInfo(id, msgType)
	//调试信息，正式屏蔽 end
	if !ok {
		return fmt.Errorf("Route--2--message %s not registered", msgType)
	}
	i := p.msgInfo[id]
	if i.msgHandler != nil {
		i.msgHandler([]interface{}{msg, userData})
	}
	if i.msgRouter != nil {
		//调用chanrpc中的Go方法
		i.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// goroutine safe
func (p *Processor) Unmarshal(data []byte) (interface{}, error) {
	if len(data) < 2 {
		return nil, errors.New("protobuf data too short")
	}
	//fmt.Printf("%+v \n", data)
	// id
	var id uint16
	if p.littleEndian {
		id = binary.LittleEndian.Uint16(data)
	} else {
		id = binary.BigEndian.Uint16(data)
	}
	//fmt.Printf(" Unmarshal id=%d \n", id)
	// if id >= uint16(len(p.msgInfo)) {
	// 	return nil, fmt.Errorf("Unmarshal--message id %v not registered", id)
	// }

	// msg
	i := p.msgInfo[id]
	//fmt.Printf("消息ID=%d \n", id)
	if i == nil {
		return nil, errors.New("protobuf data too short")
	}
	if i.msgRawHandler != nil {
		return MsgRaw{id, data[2:]}, nil
	} else {
		//这不是丢失消息
		//fmt.Printf("丟失消息******************* %d \n", id)
		//fmt.Println("丟失消息*******************", i.msgType.Name())
		msg := reflect.New(i.msgType.Elem()).Interface()
		return msg, proto.UnmarshalMerge(data[2:], msg.(proto.Message))
	}
}

// goroutine safe
func (p *Processor) Marshal(msg interface{}) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)

	// id
	_id, ok := p.msgID[msgType]
	//fmt.Printf("返回 id=======%d \n", _id)
	//fmt.Println(ok)
	//fmt.Printf("msgType =  %s", msgType)
	if !ok {
		err := fmt.Errorf("Marshal--Unmarshalmessage %s not registered", msgType)
		return nil, err
	}

	id := make([]byte, 2)
	if p.littleEndian {
		binary.LittleEndian.PutUint16(id, _id)
	} else {
		binary.BigEndian.PutUint16(id, _id)
	}
	// data
	//if _id != 12 { //&&不等于其他
	//	fmt.Printf("返回消息号%v,结构体类型%v,返回值%v\n", _id, msgType, msg)
	//}
	// data
	data, err := proto.Marshal(msg.(proto.Message))
	return [][]byte{id, data}, err
}

// goroutine safe
func (p *Processor) Range(f func(id uint16, t reflect.Type)) {
	for id, i := range p.msgInfo {
		f(uint16(id), i.msgType)
	}
}

func (p *Processor) RegisterWithId(msg proto.Message, id uint16) uint16 {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("protobuf message pointer required")
	}
	if _, ok := p.msgID[msgType]; ok {
		log.Fatal("message %s is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
	}

	i := new(MsgInfo)
	i.msgType = msgType
	//p.msgInfo = append(p.msgInfo, i)
	//id := uint16(len(p.msgInfo) )

	p.msgInfo[id] = i
	p.msgID[msgType] = id
	return id
}

func (p *Processor) GetId(msgType reflect.Type) uint16 {
	id := p.msgID[msgType]
	return id
}

func (p *Processor) GetMsgInfoMsgType(id uint16) reflect.Type {
	msgInfo := p.msgInfo[id].msgType
	return msgInfo
}

// PrintRouteInfo(id,msgType)//加到n:131左右
// 屏蔽消息号输出
func PrintRouteInfo(msgid uint16, msgType reflect.Type) {
	hideMsgIds := []uint16{60, 11} //这里填写要屏蔽输出的消息号
	for _, v := range hideMsgIds {
		if v == msgid {
			return
		}
	}

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("消息号: %d,路由: %s,时间: %s\n", msgid, msgType, timeStr)
}

//// It's dangerous to call the method on routing or marshaling (unmarshaling)
//func (p *Processor) Register(msg proto.Message) uint16 {
//	msgType := reflect.TypeOf(msg)
//	if msgType == nil || msgType.Kind() != reflect.Ptr {
//		log.Fatal("protobuf message pointer required")
//	}
//	if _, ok := p.msgID[msgType]; ok {
//		log.Fatal("message %s is already registered", msgType)
//	}
//	if len(p.msgInfo) >= math.MaxUint16 {
//		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
//	}
//
//	i := new(MsgInfo)
//	i.msgType = msgType
//	//p.msgInfo = append(p.msgInfo, i)
//	id := uint16(len(p.msgInfo))
//
//	p.msgInfo[id] = i
//
//	p.msgID[msgType] = id
//	return id
//}
