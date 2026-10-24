use crate::{
    Key, Result, invalid,
    record::{Descriptor, decode_ranks, encode_ranks},
};

#[derive(Default)]
pub struct Encoder(pub Vec<u8>);
impl Encoder {
    pub fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    pub fn u32(&mut self, value: u32) {
        self.0.extend(value.to_le_bytes());
    }
    pub fn u64(&mut self, value: u64) {
        self.0.extend(value.to_le_bytes());
    }
    pub fn bytes(&mut self, value: &[u8]) {
        self.0.extend(value);
    }
    pub fn key(&mut self, key: Key) {
        self.u64(key.consumed_tokens);
        self.bytes(&key.digest);
    }
    pub fn parent(&mut self, parent: Option<Key>) {
        self.u8(parent.is_some() as u8);
        if let Some(key) = parent {
            self.key(key);
        }
    }
    pub fn text(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.bytes(value.as_bytes());
    }
    pub fn descriptor(&mut self, descriptor: &Descriptor) {
        self.bytes(&descriptor.identity);
        self.bytes(&descriptor.fingerprint);
        self.u64(descriptor.page_tokens);
        encode_ranks(self, &descriptor.ranks);
        self.u64(descriptor.page_bytes());
        self.u64(descriptor.row_bytes());
    }
}

pub struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| invalid("length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("short payload"))?;
        self.offset = end;
        Ok(value)
    }
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub fn key(&mut self) -> Result<Key> {
        Ok(Key {
            consumed_tokens: self.u64()?,
            digest: self.array()?,
        })
    }
    pub fn parent(&mut self) -> Result<Option<Key>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.key()?)),
            _ => Err(invalid("parent flag")),
        }
    }
    pub fn text(&mut self) -> Result<&'a str> {
        let length = self.u32()? as usize;
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid("UTF-8"))
    }
    pub fn descriptor(&mut self) -> Result<Descriptor> {
        let identity = self.array()?;
        let fingerprint = self.array()?;
        let page_tokens = self.u64()?;
        let ranks = decode_ranks(self)?;
        let descriptor = Descriptor {
            identity,
            fingerprint,
            page_tokens,
            ranks,
            tenant: String::new(),
        };
        descriptor.validate()?;
        if self.u64()? != descriptor.page_bytes() || self.u64()? != descriptor.row_bytes() {
            return Err(invalid("record sizes disagree with ranks"));
        }
        Ok(descriptor)
    }
    pub fn end(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("trailing payload"))
        }
    }
}
