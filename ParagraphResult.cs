using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace org.apache.zeppelin.client {
    /// <summary>
    /// represents the paragraph execution result
    /// </summary>
    class ParagraphResult {
        public string ParagraphId { get; set; }
        public Status Status { get; set; }
        public int Progress { get; set; }
        public List<Result> Results { get; set; }
        public List<string> JobUrls { get; set; }
        public ParagraphResult(JToken paragraphJObject) {
            ParagraphId = paragraphJObject.Value<string>("id");
            Enum.TryParse(paragraphJObject.Value<string>("status"), out Status Status);
            Progress = paragraphJObject.Value<int>("progress");

            Results = new List<Result>();
            if (paragraphJObject.SelectToken("results") != null) {
                var msgArray = paragraphJObject["results"]["msg"].Children();
                foreach (var msg in msgArray) {
                    Results.Add(new Result(msg));
                }
            }

            JobUrls = new List<string>();
            if (paragraphJObject.SelectToken("runtimeInfos") != null) {
                var runtimeInfosJson = paragraphJObject.SelectToken("runtimeInfos");
                if (runtimeInfosJson.SelectToken("values") != null) {
                    var valuesArray = runtimeInfosJson["values"].Children();
                    foreach (var value in valuesArray) {
                        if (value.SelectToken("jobUrl") != null) {
                            JobUrls.Add(value.Value<string>("jobUrl"));
                        }
                    }
                }
            }
        }

        public string GetMessage() {
            var stringBuilder = new StringBuilder();
            if (Results != null) {
                foreach (var result in Results) {
                    stringBuilder.Append($"{result.Data}\n");
                }
            }
            return stringBuilder.ToString();
        }

        public override string ToString() {
            return $@"ParagraphResult{{
                    paragraphId='{ParagraphId}', 
                    status='{Status}', 
                    results={GetMessage()},
                    progress={Progress}
                    }}";
        }

    }
}
