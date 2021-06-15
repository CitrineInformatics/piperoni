.. _logging:

=======
Logging
=======

All Pipe objects have logs.
You can customize log location on instantiation of the Pipe object
using the logging_path attribute. Logging levels can also be set by stream_logging_level and
file_logging_level attributes.

In addition, you can access the logger from each transform via self.logger object. It is easy to use!
At any time from any transform, you can log at any level:

self.logger.info("I did something.")
self.logger.warning("Not sure if it worked though.")

These logs will show up as expected in their Pipe logs.
