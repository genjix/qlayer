// Mapped internally from std::error_code
exception ErrorCode {
  1: i32 what,
  2: string why
}

struct BlockHeader {
  1: i64 version,
  2: binary previous_block_hash,
  3: binary merkle,
  4: i64 timestamp,
  5: i64 bits,
  6: i64 nonce
}

typedef list<binary> HashList

struct OutputPoint {
  1: binary hash,
  2: i32 index
}

typedef OutputPoint InputPoint
typedef list<OutputPoint> OutputPointList

struct TransactionInput {
  1: OutputPoint previous_output,
  2: binary input_script,
  3: i64 sequence
}

// signed 64 bit can hold the max_money for bitcoins of 21 million
struct TransactionOutput {
  1: i64 value,
  2: binary output_script
}

struct Transaction {
  1: i64 version,
  2: i64 locktime,
  3: list<TransactionInput> inputs,
  4: list<TransactionOutput> outputs
}

struct TransactionIndex {
  1: i32 depth,
  2: i32 offset
}

service QueryService {
  bool stop(1:string secret)
  // blockchain methods
  BlockHeader block_header_by_depth(1:i32 depth)
  BlockHeader block_header_by_hash(1:binary hash)
  HashList block_transaction_hashes_by_depth(1:i32 depth)
  HashList block_transaction_hashes_by_hash(1:binary hash)
  i32 block_depth(1:binary hash)
  i32 last_depth()
  Transaction transaction(1:binary hash)
  TransactionIndex transaction_index(1:binary hash)
  InputPoint spend(1:OutputPoint outpoint)
  OutputPointList outputs(1:string address)
  // transaction pool methods
  Transaction transaction_pool_transaction(1:binary hash)
  // protocol methods
  bool broadcast_transaction(1:binary data)
}

