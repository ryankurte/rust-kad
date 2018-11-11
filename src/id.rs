

use core::hash::Hash;
use core::fmt::Debug;
use core::ops::BitXor;

/// Id trait must be implemented for viable id types
pub trait Id: Hash + BitXor + PartialEq + Eq + Ord + Clone + Send + Sync + Debug {

}


