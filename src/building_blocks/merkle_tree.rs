use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt;

// Define the MerkleNode structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MerkleNode {
    left: Option<Box<MerkleNode>>,
    right: Option<Box<MerkleNode>>,
    data: String,
}

impl MerkleNode {
    fn new(data: String) -> MerkleNode {
        MerkleNode {
            left: None,
            right: None,
            data,
        }
    }

    fn hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.data.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}

// Define the MerkleRoot structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MerkleRoot {
    root: Option<String>,
}

impl MerkleRoot {
    fn new(data: Vec<String>) -> MerkleRoot {
        let root = MerkleRoot::build_merkle_root(data);
        MerkleRoot { root }
    }

    fn build_merkle_root(data: Vec<String>) -> Option<String> {
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
                    data: combined.clone(),
                };

                new_layer.push(new_node);
            }

            current_layer = new_layer;
        }

        current_layer.first().map(|node| node.hash())
    }

    fn get_root_hash(&self) -> Option<&String> {
        self.root.as_ref()
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
        let node = MerkleNode::new("data".to_string());
        let hash = node.hash();
        // Hash length is 16 hexadecimal characters
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn test_merkle_root_building() {
        let data = vec![
            String::from_utf8(b"data1".to_vec()).unwrap(),
            String::from_utf8(b"data2".to_vec()).unwrap(),
            String::from_utf8(b"data3".to_vec()).unwrap(),
            String::from_utf8(b"data4".to_vec()).unwrap(),
            String::from_utf8(b"data5".to_vec()).unwrap(),
        ];

        let merkle_root = MerkleRoot::new(data.clone());
        assert_eq!(merkle_root.root.is_some(), true);
    }

    #[test]
    fn test_merkle_root_serialization_deserialization() {
        let data = vec![
            "data1".to_string(),
            "data2".to_string(),
            "data3".to_string(),
        ];

        let merkle_root = MerkleRoot::new(data.clone());

        let serialized = serde_json::to_string(&merkle_root).unwrap();
        let deserialized: MerkleRoot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(merkle_root.root, deserialized.root);
    }

    #[test]
    fn test_merkle_root_get_root_hash() {
        let data = vec![
            "data1".to_string(),
            "data2".to_string(),
            "data3".to_string(),
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
                    data: combined.clone(),
                };

                new_nodes.push(new_node);
            }
            nodes = new_nodes;
        }
        let expected_root_hash = nodes[0].hash();

        assert_eq!(&expected_root_hash, root_hash);
    }
}

