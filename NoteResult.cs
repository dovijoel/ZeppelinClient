using System;
using System.Collections.Generic;
using System.Text;

namespace org.apache.zeppelin.client {
    /// <summary>
    /// Represents the note execution result
    /// </summary>
    public class NoteResult {
        public string NoteId { get; set; }
        public bool IsRunning { get; set; }
        public List<ParagraphResult> ParagraphResults { get; set; }
        public NoteResult(string noteId, bool isRunning, List<ParagraphResult> paragraphResults) {
            NoteId = noteId;
            IsRunning = isRunning;
            ParagraphResults = paragraphResults;
        }

        public override string ToString() {
            return $@"NoteResult{{
                    noteId='{NoteId}',
                    isRunning='{IsRunning},
                    paragraphResults={ParagraphResults}
                    }}";
        }
    }
}
