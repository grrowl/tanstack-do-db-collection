/// Dart client for tanstack-durable-object-sync (ADR-0019).
///
/// Speaks the same wire protocol as the TS browser client — MessagePack
/// frames, one ordered stream, a single applied cursor, optimistic mutations
/// confirmed on that stream — against an unmodified SyncDurableObject.
library;

export 'src/collection.dart' show SyncCollection, WriteOutsideSubException;
export 'src/io_socket.dart' show IoSyncSocket, ioOpen;
export 'src/ir.dart';
export 'src/predicate.dart' show UnsupportedPredicateError, compilePredicate, toBooleanPredicate;
export 'src/transport.dart'
    show
        MutationRejectedException,
        ReconnectDelayFn,
        SubHandler,
        SyncSocket,
        SyncTimeoutException,
        TransportClosedException,
        WebSocketTransport,
        defaultReconnectDelay,
        maxFrameBytes;
export 'src/wire/frame_codec.dart' show decodeFrame, encodeFrame;
export 'src/wire/frames.dart';
export 'src/wire/msgpack.dart' show MsgpackDecodeError, MsgpackEncodeError, msgpackDecode, msgpackEncode;
