Restoration Verification Application
====================================

The application has two workflows.  The Singles workflow is for pinging single meters to determine if their power is on.  The Laterals workflow is for pinging groups of meters to determine if their power is off.

Redesign
--------
The first major change was prompted by a bug inherent in the design.  Because the workflows where divided into separate scripts on separate schedules but there was only one interface to ping a meter, the ping results of one workflow could overwrite the results of the other (commit 5799a835bad3f66e1f3cba32d1651463a1117d91).  This prompted me to combine the separate workflow scripts into a single implementation.  Combining and refactoring duplicate code fixed the problem and allowed me to reduce the 2045 lines in 18 files to 778 lines in 9 files (commit 7ee52b00c51bddccc6d1348b146d026ff4a491d1).

Web Services
------------
The next major change was switching from a file based batch process for meter pinging to a SOAP based web service.  Sending and receiving files through the batch interface was about 28 minute round trip plus additional time for processing.  Utilizing web services pinging each one-way meter takes on average 15 seconds and even faster for two-way meters since they require only one call to check all of them (commit 19e65d63b4433572fb895687f1c9cf41fd4336a7).

"Multi-Thread"
--------------
The most recent major change was to "multi-thread" the meter pinging code so that all the one-way meters could be pinged concurrently instead of sequentially.  During storm situations this would reduce the time to ping single meters from hours to seconds.  E.g. If there are 1600 meters to be pinged times 15 seconds for each meter is about 6.6 hours to ping them all sequentially (commit fee619f4eeff2f20c80e8b8f2e16f966b84917fd).
