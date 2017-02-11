-record(read_req, {
          mstore      :: mstore:mstore(),
          metric      :: binary(),
          time        :: non_neg_integer(),
          count       :: pos_integer(),
          compression :: snappy | none,
          req_id      :: term()
         }).
