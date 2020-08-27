using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace org.apache.zeppelin.client {
    /// <summary>
    /// Low level api for interacting with Zeppelin. Underneath, it use the zeppelin rest api.
    /// You can use this class to operate Zeppelin note/paragraph,
    /// e.g. get/add/delete/update/execute/cancel
    /// </summary>
    public class ZeppelinClient {
        private ClientConfig ClientConfig;
        private HttpClient Client;

        public ZeppelinClient(ClientConfig clientConfig) {
            ClientConfig = clientConfig;
            if (ClientConfig.UsingKnox) {
                // TODO implement Knox Configuration
                Client = new HttpClient();
            }
            else {
                Client = new HttpClient();
            }

            Client.BaseAddress = new Uri($"{clientConfig.ZeppelinRestUrl}/api");
            Client.DefaultRequestHeaders.Add("origin", "localhost");
        }
        /// <summary>
        /// Throws an excpetion if the status in the JSON object is not 'OK'
        /// </summary>
        /// <param name="jObject"></param>
        private void CheckNodeStatus(JObject jObject) {
            if (!"OK".Equals(jObject.GetValue("status").ToObject<string>(), StringComparison.OrdinalIgnoreCase)) {
                throw new Exception(jObject.GetValue("message").ToObject<string>());
            }
        }

        /// <summary>
        /// Gets the Zeppelin version
        /// </summary>
        /// <returns></returns>
        public async Task<string> GetVersion() {
            var response = await Client.GetAsync("/version");
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);
            return jObject.SelectToken("body.version").ToObject<string>();
        }

        /// <summary>
        /// Requests a new session ID. It doesn't create session (interpreter process) in zeppelin server side, 
        /// but just creates a unique session id.
        /// </summary>
        /// <param name="interpreter"></param>
        /// <returns></returns>
        public async Task<string> NewSession(string interpreter) {
            var response = await Client.PostAsync($"/session/{interpreter}", null);
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);
            return jObject.GetValue("message").ToObject<string>();
        }

        /// <summary>
        /// Stop the session(interpreter process) in zeppelin server.
        /// </summary>
        /// <param name="interpreter"></param>
        /// <param name="sessionId"></param>
        public async void StopSession(string interpreter, string sessionId) {
            var response = await Client.DeleteAsync($"/session/{interpreter}/{sessionId}");
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);
        }

        /// <summary>
        /// Get the session weburl. It is spark ui url for spark interpreter,
        /// flink web ui for flink interpreter, or may be null for the interpreter that has no weburl.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>
        public async Task<string> GetSessionWebUrl(string sessionId) {
            var response = await Client.PostAsync($"/session/{sessionId}", null);
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);

            var bodyToken = jObject.SelectToken("body");
            if (bodyToken.SelectToken("weburl") != null) {
                return bodyToken.Value<string>("weburl");
            }
            else {
                return null;
            }
        }

        /// <summary>
        /// Login to Zeppelin with username and passowrd, and throw an expception if the login fails
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        public async void Login(string userName, string password) {
            // TODO knox login
            var jObject = new JObject();
            jObject.Add("userName", userName);
            jObject.Add("password", password);
            var response = await Client.PostAsync("/login", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode) {
                throw new Exception($"Login failed, status: {response.StatusCode.ToString()}, statusText: {response.ReasonPhrase}");
            }
        }

        /// <summary>
        /// Create a new empty note with provided notePath and defaultInterpreterGroup
        /// </summary>
        /// <param name="notePath"></param>
        /// <param name="defaultInterpreterGroup"></param>
        /// <returns></returns>
        public async Task<string> CreateNote(string notePath, string defaultInterpreterGroup = "") {
            var jObject = new JObject();
            jObject.Add("name", notePath);
            jObject.Add("defaultInterpreterGroup", defaultInterpreterGroup);
            var response = await Client.PostAsync("/notebook", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return jObjectBody.SelectToken("body").ToObject<string>();
        }

        public async Task<string> CloneNote(string notePath, string noteId, string defaultInterpreterGroup = "") {
            var jObject = new JObject();
            jObject.Add("name", notePath);
            jObject.Add("defaultInterpreterGroup", defaultInterpreterGroup);
            var response = await Client.PostAsync($"/notebook/{noteId}", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return jObjectBody.SelectToken("body").ToObject<string>();
        }

        /// <summary>
        /// Delete note with provided noteId.
        /// </summary>
        /// <param name="noteId"></param>
        public async void DeleteNote(string noteId) {
            var response = await Client.DeleteAsync($"/notebook/{noteId}");
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);
        }

        /// <summary>
        /// Query <c>NoteResult</c> with provided noteId.
        /// </summary>
        /// <param name="noteId"></param>
        /// <returns></returns>
        public async Task<NoteResult> QueryNoteResult(string noteId) {
            var response = await Client.GetAsync("/notebook/{noteId}");
            response.EnsureSuccessStatusCode();
            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(stringResponse);
            CheckNodeStatus(jObject);

            var jBodyObject = jObject.SelectToken("body");
            bool isRunning = false;
            if (jBodyObject.SelectToken("info") != null) {
                var infoJson = jBodyObject.SelectToken("info");
                if (infoJson.SelectToken("isRunning") != null) {
                    isRunning = infoJson.Value<bool>("isRunning");
                }
            }

            var paragraphResults = new List<ParagraphResult>();
            if (jBodyObject.SelectToken("paragraphs") != null) {
                var paragraphs = jBodyObject["paragraphs"].Children();
                foreach (var paragraph in paragraphs) {
                    paragraphResults.Add(new ParagraphResult(paragraph));
                }
            }

            return new NoteResult(noteId, isRunning, paragraphResults);
        }

        /// <summary>
        /// Submit note to execute with provided noteId and parameters, return at once the submission is completed.
        /// You need to query <c>NoteResult</c> by yourself afterwards until note execution is completed if isBlocking is set to False
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="parameters"></param>
        /// <param name="isBlocking"></param>
        /// <returns></returns>
        public async Task<NoteResult> RunNote(string noteId, bool isBlocking, Dictionary<string, string> parameters = null) {
            if (parameters == null) {
                parameters = new Dictionary<string, string>();
            }
            var isBlockingString = isBlocking ? "true" : "false";
            var jObject = new JObject();
            jObject.Add("params", JObject.FromObject(parameters));
            var response = await Client.PostAsync($"/notebook/job/{noteId}?blocking={isBlockingString}&isolated=true", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return await QueryNoteResult(noteId);
        }

        /// <summary>
        /// Block there until note execution is completed, and throw exception if note execution is not completed
        /// Default value for timeout is -1, implying no limit
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="timeoutInMills"></param>
        /// <returns></returns>
        public NoteResult WaitUntilNoteFinished(string noteId, int timeoutInMills = -1) {
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            while (true && (timeoutInMills == -1 ? true : (DateTimeOffset.Now.ToUnixTimeMilliseconds() - start) < timeoutInMills)) {
                var noteResult = QueryNoteResult(noteId).Result;
                if (!noteResult.IsRunning) {
                    return noteResult;
                }
                Thread.Sleep(ClientConfig.QueryInterval);
            }
            throw new Exception($"Note is not finished in {timeoutInMills / 1000} seconds");
        }

        public async Task<string> AddParagraph(string noteId, string title, string text) {
            var jObject = new JObject();
            jObject.Add("title", title);
            jObject.Add("text", text);
            var response = await Client.PostAsync($"/notebook/{noteId}/paragraph", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return jObjectBody.SelectToken("body").ToObject<string>();
        }

        /// <summary>
        /// Update paragraph with specified title and text.
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="paragraphId"></param>
        /// <param name="title"></param>
        /// <param name="text"></param>
        public async void UpdateParagraph(string noteId, string paragraphId, string title, string text) {
            var jObject = new JObject();
            jObject.Add("title", title);
            jObject.Add("text", text);
            var response = await Client.PutAsync($"/notebook/{noteId}/paragraph/{paragraphId}", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);
        }

        /// <summary>
        /// Execute paragraph with parameters in specified session. If sessionId is null or empty string, then it depends on
        /// the interpreter binding mode of Note(e.g. isolated per note), otherwise it will run in the specified session.
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="paragraphId"></param>
        /// <param name="sessionId"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<ParagraphResult> RunParagraph(string noteId, string paragraphId, string sessionId = "", Dictionary<string, string> parameters = null, bool isBlocking = false) {
            if (parameters == null) parameters = new Dictionary<string, string>();
            var jObject = new JObject();
            jObject.Add("params", JObject.FromObject(parameters));
            var response = await Client.PostAsync($"/notebook/run/{noteId}/{paragraphId}?sessionId={sessionId}", new StringContent(jObject.ToString(), Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            // get paragraph result
            if (isBlocking) {
                return await WaitUntilParagraphFinish(noteId, paragraphId);
            } else {
                return await QueryParagraphResult(noteId, paragraphId);
            }
        }

        /// <summary>
        /// This used by <c>ZSession</c> for creating or reusing a paragraph for executing another piece of code.
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="maxParagraph"></param>
        /// <returns></returns>
        public async Task<string> NextSessionParagraph(string noteId, int maxParagraph) {
            var response = await Client.PostAsync($"/notebook/{noteId}/paragraph/next?maxParagraph={maxParagraph}", new StringContent("", Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return jObjectBody.Value<string>("message");
        }

        /// <summary>
        /// Cancel a running paragraph.
        /// </summary>
        /// <param name="noteId"></param>
        /// <param name="paragraphId"></param>
        public async void CancelParagraph(string noteId, string paragraphId) {
            var response = await Client.DeleteAsync($"/notebook/{noteId}/{paragraphId}");
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);
        }


        public async Task<ParagraphResult> QueryParagraphResult(string noteId, string paragraphId) {
            var response = await Client.GetAsync($"/notebook/{noteId}/paragraph/{paragraphId}");
            response.EnsureSuccessStatusCode();

            var stringResponse = await response.Content.ReadAsStringAsync();
            var jObjectBody = JObject.Parse(stringResponse);
            CheckNodeStatus(jObjectBody);

            return new ParagraphResult(jObjectBody.SelectToken("body"));
        }


        public async Task<ParagraphResult> WaitUntilParagraphFinish(string noteId, string paragraphId, int timeoutInMills = -1) {
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            while (true && (timeoutInMills == -1 ? true : (DateTimeOffset.Now.ToUnixTimeMilliseconds() - start) < timeoutInMills)) {
                var paragraphResult = await QueryParagraphResult(noteId, paragraphId);
                if (paragraphResult.Status.Equals(Status.FINISHED) || paragraphResult.Status.Equals(Status.ERROR) || paragraphResult.Status.Equals(Status.ABORT)) {
                    return paragraphResult;
                }
                Thread.Sleep(ClientConfig.QueryInterval);
            }
            throw new Exception($"Note is not finished in {timeoutInMills / 1000} seconds");
        }


        public ParagraphResult WaitUtilParagraphRunning(string noteId, string paragraphId) {
            while (true) {
                var paragraphResult = QueryParagraphResult(noteId, paragraphId).Result;
                if (paragraphResult.Status.Equals(Status.RUNNING)) { // possibility of missing window where it is RUNNING?
                    return paragraphResult;
                }
                Thread.Sleep(ClientConfig.QueryInterval);
            }
        }
    }
}
