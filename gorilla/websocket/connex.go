package websocket

import (
	"encoding/binary"
	"errors"
)

func (c *Conn) WriteMessageEx(messageType int, datas ...[]byte) error {
	if len(datas) == 0 {
		return nil
	} else if len(datas) == 1 {
		//return this.WriteMessage(messageType, datas[0])
	}

	if !c.isServer {
		return errors.New("conn isn't server!")
	}

	if !isData(messageType) {
		return errBadWriteOpCode
	}
	var maxPayloadSize int
	for _, data := range datas {
		maxPayloadSize += maxFrameHeaderSize
		maxPayloadSize += len(data)
	}
	buf := make([]byte, 0, maxPayloadSize)
	dataLenBuf := make([]byte, 8)
	for _, data := range datas {
		b0 := byte(messageType)
		b0 |= finalBit
		b1 := byte(0)
		length := len(data)
		switch {
		case length >= 65536:
			b1 |= 127
			buf = append(buf, b0, b1)
			binary.BigEndian.PutUint64(dataLenBuf, uint64(length))
			buf = append(buf, dataLenBuf...)
		case length > 125:
			b1 |= 126
			buf = append(buf, b0, b1)
			binary.BigEndian.PutUint16(dataLenBuf, uint16(length))
			buf = append(buf, dataLenBuf[:2]...)
		default:
			b1 |= byte(length)
			buf = append(buf, b0, b1)
		}
		buf = append(buf, data...)
	}
	<-c.mu
	defer func() { c.mu <- true }()

	if c.writeErr != nil {
		return c.writeErr
	} else if c.closeSent {
		return ErrCloseSent
	}

	if !c.writeDeadline.Equal(c.prewriteDeadline) {
		c.conn.SetWriteDeadline(c.writeDeadline)
		c.prewriteDeadline = c.writeDeadline
	}

	n, err := c.conn.Write(buf)
	if n != 0 && n != len(buf) {
		c.conn.Close()
	}
	return err
}

//func (c *Conn) WriteMessageEx(messageType int, datas ...[]byte) error {
//	if !isData(messageType) {
//		return errBadWriteOpCode
//	}

//	totalen := 0
//	for _, data := range datas {
//		l := len(data)
//		totalen += l + maxFrameHeaderSize
//	}

//	buf := make([]byte, 0, totalen)

//	for _, data := range datas {
//		length := len(data)
//		b0 := byte(messageType) | finalBit
//		b1 := byte(0)
//		if !c.isServer {
//			b1 |= maskBit
//		}

//		b := make([]byte, 8)
//		switch {
//		case length >= 65536:
//			buf = append(buf, b0, (b1 | 127))
//			buf = append(buf, b...)
//			binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(length))
//		case length > 125:
//			buf = append(buf, b0, (b1 | 126))
//			buf = append(buf, b[:2]...)
//			binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(length))
//		default:
//			buf = append(buf, b0, (b1 | byte(length)))
//		}
//		buf = append(buf, data...)
//	}

//	if c.closeSent {
//		return ErrCloseSent
//	}

//	n, err := c.conn.Write(buf)
//	if n != 0 && n != len(buf) {
//		c.conn.Close()
//	}
//	return err
//}
