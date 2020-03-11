use bytes::BytesMut;
use mqtt3::{self, MqttRead, MqttWrite, Packet};
use std::io::{self, Cursor, ErrorKind};
use tokio_util::codec::{Decoder, Encoder};

const MQTT_MIN_HEADER_SIZE: usize = 2;

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // NOTE: `decode` might be called with `buf.len == 0` when previous
        // `decode` reads all the bytes in the stream.  We should return
        // `Ok(None)` in those cases or else the `read` call will return
        // `Ok(0)` (translated to `UnexpectedEOF`) by `byteorder` crate.
        // `read` returning `Ok(0)` happens when the specified buffer has 0
        // bytes in len.
        //
        // See: https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
        if buf.len() < MQTT_MIN_HEADER_SIZE {
            return Ok(None);
        }

        let (packet, len) = {
            let mut buf_ref = buf.as_ref();
            match buf_ref.read_packet_with_len() {
                Err(e) => {
                    let errmsg = e.to_string();
                    return if let mqtt3::Error::Io(e) = e {
                        match e.kind() {
                            ErrorKind::TimedOut
                            | ErrorKind::WouldBlock
                            | ErrorKind::UnexpectedEof => Ok(None),
                            _ => Err(Self::Error::new(e.kind(), errmsg)),
                        }
                    } else {
                        Err(Self::Error::new(ErrorKind::Other, errmsg))
                    };
                }
                Ok(v) => v,
            }
        };

        // NOTE: It's possible that `decode` is called before `buf` is full
        // enough to frame the raw bytes in a packet.  In that case, we return
        // `Ok(None)` for the next `decode` is called, there will be more
        // bytes in `buf`, hopefully enough to frame the packet.
        if buf.len() < len {
            return Ok(None);
        }

        let _ = buf.split_to(len);

        Ok(Some(packet))
    }
}

impl Encoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, msg: Packet, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let mut stream = Cursor::new(Vec::new());

        // TODO: Implement `write_packet` for `&mut BytesMut`
        if let Err(e) = stream.write_packet(&msg) {
            return Err(Self::Error::new(io::ErrorKind::Other, e));
        }

        buf.extend(stream.get_ref());

        Ok(())
    }
}
