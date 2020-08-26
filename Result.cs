using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace org.apache.zeppelin.client {
    /// <summary>
    /// Represent one segment of result of paragraph. The result of paragraph could consists of
    /// multiple Results.
    /// </summary>
    class Result {
        public string Type { get; set; }
        public string Data { get; set; }
        public Result(JToken jObject) {
            Type = jObject.Value<string>("type");
            Data = jObject.Value<string>("data");
        }

        public Result(string type, string data) {
            Type = type;
            Data = data;
        }

        public void AppendData(string newData) {
            Data += newData;
        }

        public override string ToString() {
            return $@"Result{{
                    type='{Type}',
                    data='{Data}'
                    }}";
        }
    }
}
