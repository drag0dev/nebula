use serde::{Serialize, Deserialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt;
use anyhow::{Result, Context};
use super::BINCODE_OPTIONS;
use bincode::Options;

// Define the MerkleNode structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleNode {
    left: Option<Box<MerkleNode>>,
    right: Option<Box<MerkleNode>>,
    data: Vec<u8>,
}

impl MerkleNode {
    pub fn new(data: Vec<u8>) -> MerkleNode {
        MerkleNode {
            left: None,
            right: None,
            data,
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.data.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}

// Define the MerkleRoot structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleRoot {
    root: Option<Vec<u8>>,
}

impl MerkleRoot {
    pub fn new(data: Vec<Vec<u8>>) -> MerkleRoot {
        let root = MerkleRoot::build_merkle_root(data);
        MerkleRoot { root }
    }

    pub fn build_merkle_root(data: Vec<Vec<u8>>) -> Option<Vec<u8>> {
        let nodes: Vec<MerkleNode> = data.into_iter().map(MerkleNode::new).collect();
        let mut current_layer = nodes;

        while current_layer.len() > 1 {
            let mut new_layer = Vec::new();

            for i in (0..current_layer.len()).step_by(2) {
                let left = current_layer[i].clone();
                let right = if i + 1 < current_layer.len() {
                    current_layer[i + 1].clone()
                } else {
                    left.clone()
                };

                let combined = format!("{}{}", left.hash(), right.hash());
                let new_node = MerkleNode {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    data: combined.as_bytes().to_vec(),
                };

                new_layer.push(new_node);
            }

            current_layer = new_layer;
        }

        current_layer.first().map(|node| node.data.clone())
    }

    pub fn get_root_hash(&self) -> Option<&Vec<u8>> {
        self.root.as_ref()
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(BINCODE_OPTIONS
            .serialize(&self)
            .context("serializing merkle tree")?)
    }

    pub fn deserialize(data: &[u8]) -> Result<MerkleRoot> {
        Ok(BINCODE_OPTIONS
            .deserialize(data)
            .context("deserializing merkle tree")?)
    }
}

impl fmt::Display for MerkleRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MerkleRoot {{ root: {:?} }}", self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_node_hash() {
        let node = MerkleNode::new(vec![1, 2, 3]);
        let hash = node.hash();
        // Hash length is 16 hexadecimal characters
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn test_merkle_root_building() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
            vec![10, 11, 12],
            vec![13, 14, 15],
        ];

        let merkle_root = MerkleRoot::new(data.clone());
        assert_eq!(merkle_root.root.is_some(), true);
    }

    #[test]
    fn test_merkle_root_serialization_deserialization() {
        use bincode::{serialize, deserialize};

        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
        ];

        let merkle_root = MerkleRoot::new(data.clone());

        let serialized = serialize(&merkle_root).unwrap();
        let deserialized: MerkleRoot = deserialize(&serialized).unwrap();

        assert_eq!(merkle_root.root, deserialized.root);
    }

    #[test]
    fn test_merkle_root_get_root_hash() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
        ];

        let merkle_root = MerkleRoot::new(data.clone());
        let root_hash = merkle_root.get_root_hash().unwrap();

        // Manually calculate the root hash
        let mut nodes: Vec<MerkleNode> = data.iter().map(|d| MerkleNode::new(d.clone())).collect();
        while nodes.len() > 1 {
            let mut new_nodes = Vec::new();
            for i in (0..nodes.len()).step_by(2) {
                let left = nodes[i].clone();
                let right = if i + 1 < nodes.len() {
                    nodes[i + 1].clone()
                } else {
                    left.clone()
                };

                let combined = format!("{}{}", left.hash(), right.hash());
                let new_node = MerkleNode {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    data: combined.as_bytes().to_vec(),
                };

                new_nodes.push(new_node);
            }
            nodes = new_nodes;
        }
        let expected_root_hash = nodes[0].data.clone();

        assert_eq!(&expected_root_hash, root_hash);
    }
}
