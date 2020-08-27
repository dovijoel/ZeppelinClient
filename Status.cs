using System;
using System.Collections.Generic;
using System.Text;

namespace org.apache.zeppelin.client {
    /// <summary>
    /// Job status.
    /// UNKNOWN - Job is not found in remote
    /// READY - Job is not running, ready to run.
    /// PENDING - Job is submitted to scheduler.but not running yet
    /// RUNNING - Job is running.
    /// FINISHED - Job finished run.with success
    /// ERROR - Job finished run.with error
    /// ABORT - Job finished by abort
    /// </summary>
    public enum Status {
        UNKNOWN, READY, PENDING, RUNNING, FINISHED, ERROR, ABORT
    }
}
