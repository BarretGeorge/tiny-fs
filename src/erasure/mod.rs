use std::io;

#[derive(Debug)]
pub struct ErasureCode {
    pub data_blocks: usize,
    pub parity_blocks: usize,
}

impl ErasureCode {
    pub fn new(data_blocks: usize, parity_blocks: usize) -> Self {
        Self {
            data_blocks,
            parity_blocks,
        }
    }

    pub fn total_blocks(&self) -> usize {
        self.data_blocks + self.parity_blocks
    }

    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, ErasureError> {
        let block_size = self.calculate_block_size(data.len());
        let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(self.total_blocks());

        for i in 0..self.data_blocks {
            let start = i * block_size;
            let end = (start + block_size).min(data.len());
            let mut block = data[start..end].to_vec();

            if block.len() < block_size {
                block.resize(block_size, 0);
            }

            blocks.push(block);
        }

        for _ in 0..self.parity_blocks {
            blocks.push(vec![0u8; block_size]);
        }

        for i in 0..self.parity_blocks {
            let parity_idx = self.data_blocks + i;
            let mut parity = vec![0u8; block_size];

            for j in 0..self.data_blocks {
                let block = &blocks[j];
                for (k, byte) in block.iter().enumerate() {
                    if k < parity.len() {
                        parity[k] ^= byte;
                    }
                }
            }

            for k in 0..block_size {
                for m in 0..i {
                    parity[k] = self.gf_mul(parity[k], self.gf_pow(2, m));
                }
            }

            blocks[parity_idx] = parity;
        }

        Ok(blocks)
    }

    pub fn decode(&self, blocks: &[Vec<u8>]) -> Result<Vec<u8>, ErasureError> {
        if blocks.len() < self.data_blocks {
            return Err(ErasureError::InsufficientData {
                required: self.data_blocks,
                available: blocks.len(),
            });
        }

        let block_size = blocks.first().map(|b| b.len()).unwrap_or(0);
        let mut data = Vec::with_capacity(self.data_blocks * block_size);

        for i in 0..self.data_blocks {
            if i < blocks.len() {
                data.extend_from_slice(&blocks[i]);
            }
        }

        while data.last() == Some(&0) {
            data.pop();
        }

        Ok(data)
    }

    pub fn reconstruct(
        &self,
        blocks: &[Vec<u8>],
        missing_indices: &[usize],
    ) -> Result<Vec<Vec<u8>>, ErasureError> {
        let mut recovered = blocks.to_vec();
        let block_size = blocks.first().map(|b| b.len()).unwrap_or(0);

        for &missing_idx in missing_indices {
            if missing_idx >= self.total_blocks() {
                continue;
            }

            let mut recovered_block = vec![0u8; block_size];

            for i in 0..self.data_blocks {
                if missing_indices.contains(&i) {
                    continue;
                }

                let src_idx = if i < missing_idx {
                    i
                } else {
                    i + self.parity_blocks
                };
                if src_idx < blocks.len() {
                    for (j, byte) in blocks[src_idx].iter().enumerate() {
                        if j < block_size {
                            recovered_block[j] ^= byte;
                        }
                    }
                }
            }

            if missing_idx >= self.data_blocks {
                for k in 0..block_size {
                    for m in 0..(missing_idx - self.data_blocks) {
                        recovered_block[k] = self.gf_mul(recovered_block[k], self.gf_pow(2, m));
                    }
                }
            }

            while recovered.len() <= missing_idx {
                recovered.push(vec![]);
            }
            recovered[missing_idx] = recovered_block;
        }

        Ok(recovered)
    }

    pub fn verify(&self, blocks: &[Vec<u8>]) -> Result<bool, ErasureError> {
        if blocks.len() < self.total_blocks() {
            return Ok(false);
        }

        let block_size = blocks.first().map(|b| b.len()).unwrap_or(0);

        for parity_idx in self.data_blocks..self.total_blocks() {
            let mut computed = vec![0u8; block_size];

            for j in 0..self.data_blocks {
                let block = &blocks[j];
                for (k, byte) in block.iter().enumerate() {
                    if k < computed.len() {
                        computed[k] ^= byte;
                    }
                }
            }

            if blocks[parity_idx] != computed {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn calculate_block_size(&self, total_size: usize) -> usize {
        (total_size + self.data_blocks - 1) / self.data_blocks
    }

    fn gf_mul(&self, a: u8, b: u8) -> u8 {
        let mut result = 0u8;
        let mut aa = a;
        let mut bb = b;

        while bb != 0 {
            if bb & 1 != 0 {
                result ^= aa;
            }
            let hi = aa & 0x80;
            aa <<= 1;
            if hi != 0 {
                aa ^= 0x1b;
            }
            bb >>= 1;
        }

        result
    }

    fn gf_pow(&self, base: u8, exp: usize) -> u8 {
        if exp == 0 {
            return 1;
        }

        let mut result = base;
        for _ in 1..exp {
            result = self.gf_mul(result, base);
        }

        result
    }
}

#[derive(Debug)]
pub enum ErasureError {
    IoError(io::Error),
    InsufficientData { required: usize, available: usize },
    ReconstructionFailed,
    VerificationFailed,
}

impl From<io::Error> for ErasureError {
    fn from(err: io::Error) -> Self {
        ErasureError::IoError(err)
    }
}

impl std::fmt::Display for ErasureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErasureError::IoError(e) => write!(f, "IO error: {}", e),
            ErasureError::InsufficientData {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient data: required {}, available {}",
                    required, available
                )
            }
            ErasureError::ReconstructionFailed => write!(f, "Failed to reconstruct data"),
            ErasureError::VerificationFailed => write!(f, "Verification failed"),
        }
    }
}

impl std::error::Error for ErasureError {}

pub struct BitrotAlgo;

impl BitrotAlgo {
    pub fn checksum(data: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(data))
    }

    pub fn verify(data: &[u8], expected_checksum: &str) -> bool {
        Self::checksum(data) == expected_checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erasure_basic() {
        let ec = ErasureCode::new(4, 2);
        let data = b"Hello, World! This is a test of erasure coding.";

        let encoded = ec.encode(data).unwrap();
        assert_eq!(encoded.len(), 6);

        let decoded = ec.decode(&encoded[..4]).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_erasure_reconstruct() {
        let ec = ErasureCode::new(4, 2);
        let data = b"Test data for reconstruction";

        let encoded = ec.encode(data).unwrap();
        let recovered = ec.reconstruct(&encoded, &[2, 5]).unwrap();

        assert_eq!(recovered.len(), 6);
    }
}
