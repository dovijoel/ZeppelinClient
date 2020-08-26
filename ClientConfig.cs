using System;

namespace org.apache.zeppelin.client {
    public class ClientConfig {
        public string ZeppelinRestUrl { get; set; }
        public int QueryInterval { get; set; }
        public bool UsingKnox { get; set; }

        public ClientConfig(string zeppelinRestUrl, int queryInterval = 1000, bool usingKnox = false) {
            ZeppelinRestUrl = zeppelinRestUrl;
            QueryInterval = queryInterval;
            UsingKnox = usingKnox;
        }
    }
}
