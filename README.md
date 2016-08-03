## Message layout

	+--------------+ +--------+ +---------+
	| message_size | | offset | | message |
	|              | |        | |         |
	|    int 32    | | int 64 | | n bytes |
	+--------------+ +--------+ +---------+

`message_size` the size of the entire message, including header and body
`offset` the offset of the message in the stream or set
`message` the actual content of the message

## Directory layout

	./
	./.lock      supporting the claim of an strand process
	./*.str      stream data files
