﻿using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Extensions;

namespace Sparrow.Json
{
    public class AsyncBlittableJsonTextWriter : IAsyncDisposable
    {
        protected readonly JsonOperationContext _context;
        protected readonly Stream _stream;
        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        public static readonly byte[] NaNBuffer = { (byte)'"', (byte)'N', (byte)'a', (byte)'N', (byte)'"' };

        public static readonly byte[] PositiveInfinityBuffer =
        {
            (byte)'"', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };

        public static readonly byte[] NegativeInfinityBuffer =
        {
            (byte)'"', (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };

        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        internal static readonly byte[] EscapeCharacters;
        public static readonly byte[][] ControlCodeEscapes;

        private readonly UnmanagedMemory _buffer;
        private readonly UnmanagedMemory _auxiliarBuffer;

        private int _pos;
        private readonly JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
        private readonly JsonOperationContext.MemoryBuffer.ReturnBuffer _returnAuxiliarBuffer;

        static AsyncBlittableJsonTextWriter()
        {
            ControlCodeEscapes = new byte[32][];

            for (int i = 0; i < 32; i++)
            {
                ControlCodeEscapes[i] = Encodings.Utf8.GetBytes(i.ToString("X4"));
            }

            EscapeCharacters = new byte[256];
            for (int i = 0; i < 32; i++)
                EscapeCharacters[i] = 0;

            for (int i = 32; i < EscapeCharacters.Length; i++)
                EscapeCharacters[i] = 255;

            EscapeCharacters[(byte)'\b'] = (byte)'b';
            EscapeCharacters[(byte)'\t'] = (byte)'t';
            EscapeCharacters[(byte)'\n'] = (byte)'n';
            EscapeCharacters[(byte)'\f'] = (byte)'f';
            EscapeCharacters[(byte)'\r'] = (byte)'r';
            EscapeCharacters[(byte)'\\'] = (byte)'\\';
            EscapeCharacters[(byte)'/'] = (byte)'/';
            EscapeCharacters[(byte)'"'] = (byte)'"';
        }

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;

            _returnBuffer = context.GetMemoryBuffer(out var pinnedBuffer);
            _buffer = pinnedBuffer.Memory;

            _returnAuxiliarBuffer = context.GetMemoryBuffer(32, out pinnedBuffer);
            _auxiliarBuffer = pinnedBuffer.Memory;
        }

        public int Position => _pos;

        public unsafe override string ToString()
        {
            return Encodings.Utf8.GetString(_buffer.Address, _pos);
        }

        public async ValueTask WriteObjectAsync(BlittableJsonReaderObject obj, CancellationToken token = default)
        {
            if (obj == null)
            {
                await WriteNullAsync(token).ConfigureAwait(false);
                return;
            }

            await WriteStartObjectAsync(token).ConfigureAwait(false);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffer = obj.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < buffer.Size; i++)
                {
                    if (i != 0)
                    {
                        await WriteCommaAsync(token).ConfigureAwait(false);
                    }

                    unsafe
                    {
                        obj.GetPropertyByIndex(buffer.Properties[i], ref prop);
                    }

                    await WritePropertyNameAsync(prop.Name, token).ConfigureAwait(false);

                    await WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value, token).ConfigureAwait(false);
                }
            }

            await WriteEndObjectAsync(token).ConfigureAwait(false);
        }

        private async ValueTask WriteArrayToStreamAsync(BlittableJsonReaderArray array, CancellationToken token = default)
        {
            await WriteStartArrayAsync(token).ConfigureAwait(false);
            var length = array.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = array.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    await WriteCommaAsync(token).ConfigureAwait(false);
                }
                // write field value
                await WriteValueAsync(propertyValueAndType.Item2, propertyValueAndType.Item1, token).ConfigureAwait(false);
            }
            await WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        public async ValueTask WriteValueAsync(BlittableJsonToken jsonToken, object val, CancellationToken token = default)
        {
            switch (jsonToken)
            {
                case BlittableJsonToken.String:
                    await WriteStringAsync((LazyStringValue)val, token: token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Integer:
                    await WriteIntegerAsync((long)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.StartArray:
                    await WriteArrayToStreamAsync((BlittableJsonReaderArray)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = (BlittableJsonReaderObject)val;
                    await WriteObjectAsync(blittableJsonReaderObject, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.CompressedString:
                    await WriteStringAsync((LazyCompressedStringValue)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.LazyNumber:
                    await WriteDoubleAsync((LazyNumberValue)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Boolean:
                    await WriteBoolAsync((bool)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Null:
                    await WriteNullAsync(token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.RawBlob:
                    var blob = (BlittableJsonReaderObject.RawBlob)val;
                    await WriteRawStringAsync(blob.Memory, blob.Length, token).ConfigureAwait(false);
                    break;

                default:
                    throw new DataMisalignedException($"Unidentified Type {jsonToken}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteDateTimeAsync(DateTime value, bool isUtc, CancellationToken token = default)
        {
            int size;
            unsafe
            {
                size = value.GetDefaultRavenFormat(_auxiliarBuffer.Address, _auxiliarBuffer.Size, isUtc);
            }

            await WriteRawStringWhichMustBeWithoutEscapeCharsAsync(_auxiliarBuffer, size, token).ConfigureAwait(false);
        }

        public async ValueTask WriteStringAsync(string str, bool skipEscaping = false, CancellationToken token = default)
        {
            using (var lazyStr = _context.GetLazyString(str))
            {
                await WriteStringAsync(lazyStr, skipEscaping, token).ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStringAsync(LazyStringValue str, bool skipEscaping = false, CancellationToken token = default)
        {
            if (str == null)
            {
                await WriteNullAsync(token).ConfigureAwait(false);
                return;
            }

            var size = str.Size;

            if (size == 1 && str.IsControlCodeCharacter(out var b))
            {
                await WriteStringAsync($@"\u{b:X4}", skipEscaping: true, token).ConfigureAwait(false);
                return;
            }

            var strBuffer = str.MemoryBuffer;
            var escapeSequencePos = size;
            int numberOfEscapeSequences;
            unsafe
            {
                numberOfEscapeSequences = skipEscaping ? 0 : BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            }

            // We ensure our buffer will have enough space to deal with the whole string.

            const int NumberOfQuotesChars = 2; // for " "

            int bufferSize = 2 * numberOfEscapeSequences + size + NumberOfQuotesChars;
            if (bufferSize >= JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                await UnlikelyWriteLargeStringAsync(strBuffer, size, numberOfEscapeSequences, escapeSequencePos, token).ConfigureAwait(false); // OK, do it the slow way.
                return;
            }

            await EnsureBufferAsync(size + NumberOfQuotesChars, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }

            if (numberOfEscapeSequences == 0)
            {
                // PERF: Fast Path.
                await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);
            }
            else
            {
                await UnlikelyWriteEscapeSequencesAsync(strBuffer, size, numberOfEscapeSequences, escapeSequencePos, token).ConfigureAwait(false);
            }

            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }
        }

        private async ValueTask UnlikelyWriteEscapeSequencesAsync(UnmanagedMemory strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos, CancellationToken token = default)
        {
            // We ensure our buffer will have enough space to deal with the whole string.
            int bufferSize = 2 * numberOfEscapeSequences + size + 1;

            await EnsureBufferAsync(bufferSize, token).ConfigureAwait(false);

            var ptr = strBuffer;
            var buffer = _buffer;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;

                int bytesToSkip;
                unsafe
                {
                    bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr.Address, ref escapeSequencePos);
                }

                if (bytesToSkip > 0)
                {
                    await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                    strBuffer = strBuffer.Slice(bytesToSkip);
                    size -= bytesToSkip;
                }

                byte escapeCharacter;
                unsafe
                {
                    escapeCharacter = *strBuffer.Address;
                }

                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(buffer, escapeCharacter, token).ConfigureAwait(false);

                size--;
            }

            Debug.Assert(size >= 0);

            // write remaining (or full string) to the buffer in one shot
            if (size > 0)
                await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);
        }

        private async ValueTask UnlikelyWriteLargeStringAsync(UnmanagedMemory strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos, CancellationToken token = default)
        {
            var ptr = strBuffer;

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }

            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;

                int bytesToSkip;
                unsafe
                {
                    bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr.Address, ref escapeSequencePos);
                }

                await UnlikelyWriteLargeRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                strBuffer = strBuffer.Slice(bytesToSkip);
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                byte b;
                unsafe
                {
                    b = *strBuffer.Address;
                }

                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(_buffer, b, token).ConfigureAwait(false);
            }

            // write remaining (or full string) to the buffer in one shot
            await UnlikelyWriteLargeRawStringAsync(strBuffer, size, token).ConfigureAwait(false);

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask WriteEscapeCharacterAsync(UnmanagedMemory buffer, byte b, CancellationToken token = default)
        {
            byte r = EscapeCharacters[b];
            if (r == 0)
            {
                await EnsureBufferAsync(6, token).ConfigureAwait(false);

                unsafe
                {
                    buffer.Address[_pos++] = (byte)'\\';
                    buffer.Address[_pos++] = (byte)'u';
                    fixed (byte* esc = ControlCodeEscapes[b])
                        Memory.Copy(buffer.Address + _pos, esc, 4);
                }

                _pos += 4;
                return;
            }

            if (r != 255)
            {
                await EnsureBufferAsync(2, token).ConfigureAwait(false);

                unsafe
                {
                    buffer.Address[_pos++] = (byte)'\\';
                    buffer.Address[_pos++] = r;
                }

                return;
            }

            ThrowInvalidEscapeCharacter(b);
        }

        private void ThrowInvalidEscapeCharacter(byte b)
        {
            throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStringAsync(LazyCompressedStringValue str, CancellationToken token = default)
        {
            var strBuffer = str.DecompressToUnmanagedMemory(out AllocatedMemoryData allocated, _context);

            try
            {
                var size = str.UncompressedSize;
                var escapeSequencePos = str.CompressedSize;
                int numberOfEscapeSequences;
                unsafe
                {
                    numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                }

                // We ensure our buffer will have enough space to deal with the whole string.
                int bufferSize = 2 * numberOfEscapeSequences + size + 2;
                if (bufferSize >= JsonOperationContext.MemoryBuffer.DefaultSize)
                    goto WriteLargeCompressedString; // OK, do it the slow way instead.

                await EnsureBufferAsync(bufferSize, token).ConfigureAwait(false);
                unsafe
                {
                    _buffer.Address[_pos++] = Quote;
                }

                while (numberOfEscapeSequences > 0)
                {
                    numberOfEscapeSequences--;

                    int bytesToSkip;
                    unsafe
                    {
                        bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                    }

                    await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                    strBuffer = strBuffer.Slice(bytesToSkip);
                    size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                    byte b;
                    unsafe
                    {
                        b = *strBuffer.Address;
                    }

                    strBuffer = strBuffer.Slice(1);

                    await WriteEscapeCharacterAsync(_buffer, b, token).ConfigureAwait(false);
                }

                // write remaining (or full string) to the buffer in one shot
                await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);
                unsafe
                {
                    _buffer.Address[_pos++] = Quote;
                }

                return;

            WriteLargeCompressedString:
                await UnlikelyWriteLargeStringAsync(numberOfEscapeSequences, str, escapeSequencePos, strBuffer, size, token).ConfigureAwait(false);
            }
            finally
            {
                if (allocated != null) //precaution
                    _context.ReturnMemory(allocated);
            }
        }

        private async ValueTask UnlikelyWriteLargeStringAsync(int numberOfEscapeSequences, LazyCompressedStringValue lsv, int escapeSequencePos, UnmanagedMemory strBuffer, int size, CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }

            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;

                int bytesToSkip;
                unsafe
                {
                    bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(lsv.Buffer, ref escapeSequencePos);
                }

                await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                strBuffer = strBuffer.Slice(bytesToSkip);
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                byte b;
                unsafe
                {
                    b = *strBuffer.Address;
                }

                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(_buffer, b, token).ConfigureAwait(false);
            }

            // write remaining (or full string) to the buffer in one shot
            await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteRawStringWhichMustBeWithoutEscapeCharsAsync(UnmanagedMemory buffer, int size, CancellationToken token = default)
        {
            await EnsureBufferAsync(size + 2, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }

            await WriteRawStringAsync(buffer, size, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Quote;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask WriteRawStringAsync(UnmanagedMemory buffer, int size, CancellationToken token = default)
        {
            if (size < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                await EnsureBufferAsync(size, token).ConfigureAwait(false);

                unsafe
                {
                    Memory.Copy(_buffer.Address + _pos, buffer.Address, size);
                }

                _pos += size;
                return;
            }

            await UnlikelyWriteLargeRawStringAsync(buffer, size, token).ConfigureAwait(false);
        }

        private async ValueTask UnlikelyWriteLargeRawStringAsync(UnmanagedMemory buffer, int size, CancellationToken token = default)
        {
            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < size)
            {
                var amountToCopy = Math.Min(size - posInStr, JsonOperationContext.MemoryBuffer.DefaultSize);
                await FlushAsync(token).ConfigureAwait(false);

                unsafe
                {
                    Memory.Copy(_buffer.Address, buffer.Address + posInStr, amountToCopy);
                }

                posInStr += amountToCopy;
                _pos = amountToCopy;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStartObjectAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = StartObject;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteEndArrayAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = EndArray;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStartArrayAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = StartArray;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteEndObjectAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = EndObject;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask EnsureBufferAsync(int len, CancellationToken token = default)
        {
            if (len >= JsonOperationContext.MemoryBuffer.DefaultSize)
                ThrowValueTooBigForBuffer(len);
            if (_pos + len < JsonOperationContext.MemoryBuffer.DefaultSize)
                return;

            await FlushAsync(token).ConfigureAwait(false);
        }

        public
#if !NETSTANDARD2_0
            async
#endif
            ValueTask FlushAsync(CancellationToken token = default)
        {
            if (_stream == null)
                ThrowStreamClosed();

#if NETSTANDARD2_0
            if (_pos == 0)
                return new ValueTask();

            _stream.Write(_buffer.Memory.Span.Slice(0, _pos)); // TODO [ppekrol]
            _pos = 0;
            return new ValueTask();
#else
            if (_pos == 0)
                return;

            await _stream.WriteAsync(_buffer.Memory.Slice(0, _pos), token).ConfigureAwait(false);
            _pos = 0;
#endif
        }

        private static void ThrowValueTooBigForBuffer(int len)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len", len, "Length value too big: " + len);
        }

        private void ThrowStreamClosed()
        {
            throw new ObjectDisposedException("The stream was closed already.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteNullAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(4, token).ConfigureAwait(false);
            unsafe
            {
                for (int i = 0; i < 4; i++)
                {
                    _buffer.Address[_pos++] = NullBuffer[i];
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteBoolAsync(bool val, CancellationToken token = default)
        {
            await EnsureBufferAsync(5, token).ConfigureAwait(false);
            var buffer = val ? TrueBuffer : FalseBuffer;
            unsafe
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    _buffer.Address[_pos++] = buffer[i];
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteCommaAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Comma;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(LazyStringValue prop, CancellationToken token = default)
        {
            await WriteStringAsync(prop, token: token).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Colon;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(string prop, CancellationToken token = default)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            await WriteStringAsync(lazyProp).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Colon;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(StringSegment prop, CancellationToken token = default)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            await WriteStringAsync(lazyProp).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = Colon;
            }
        }

        public async ValueTask WriteIntegerAsync(long val, CancellationToken token = default)
        {
            if (val == 0)
            {
                await EnsureBufferAsync(1, token).ConfigureAwait(false);
                unsafe
                {
                    _buffer.Address[_pos++] = (byte)'0';
                }

                return;
            }

            var localBuffer = _auxiliarBuffer;

            int idx = 0;
            var negative = false;
            var isLongMin = false;
            if (val < 0)
            {
                negative = true;
                if (val == long.MinValue)
                {
                    isLongMin = true;
                    val = long.MaxValue;
                }
                else
                    val = -val; // value is positive now.
            }

            unsafe
            {
                do
                {
                    var v = val % 10;
                    if (isLongMin)
                    {
                        isLongMin = false;
                        v += 1;
                    }

                    localBuffer.Address[idx++] = (byte)('0' + v);
                    val /= 10;
                }
                while (val != 0);

                if (negative)
                    localBuffer.Address[idx++] = (byte)'-';
            }

            await EnsureBufferAsync(idx, token).ConfigureAwait(false);

            var buffer = _buffer;
            int auxPos = _pos;

            unsafe
            {
                do
                {
                    buffer.Address[auxPos++] = localBuffer.Address[--idx];
                }
                while (idx > 0);
            }

            _pos = auxPos;
        }

        public async ValueTask WriteDoubleAsync(LazyNumberValue val, CancellationToken token = default)
        {
            if (val.IsNaN())
            {
                await WriteBufferForAsync(NaNBuffer, token).ConfigureAwait(false);
                return;
            }

            if (val.IsPositiveInfinity())
            {
                await WriteBufferForAsync(PositiveInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            if (val.IsNegativeInfinity())
            {
                await WriteBufferForAsync(NegativeInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            var lazyStringValue = val.Inner;
            await EnsureBufferAsync(lazyStringValue.Size, token).ConfigureAwait(false);
            await WriteRawStringAsync(lazyStringValue.MemoryBuffer, lazyStringValue.Size, token).ConfigureAwait(false);
        }

        public async ValueTask WriteBufferForAsync(byte[] buffer, CancellationToken token = default)
        {
            await EnsureBufferAsync(buffer.Length, token).ConfigureAwait(false);
            unsafe
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    _buffer.Address[_pos++] = buffer[i];
                }
            }
        }

        public async ValueTask WriteDoubleAsync(double val, CancellationToken token = default)
        {
            if (double.IsNaN(val))
            {
                await WriteBufferForAsync(NaNBuffer, token).ConfigureAwait(false);
                return;
            }

            if (double.IsPositiveInfinity(val))
            {
                await WriteBufferForAsync(PositiveInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            if (double.IsNegativeInfinity(val))
            {
                await WriteBufferForAsync(NegativeInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            using (var lazyStr = _context.GetLazyString(val.ToString(CultureInfo.InvariantCulture)))
            {
                await EnsureBufferAsync(lazyStr.Size, token).ConfigureAwait(false);
                await WriteRawStringAsync(lazyStr.MemoryBuffer, lazyStr.Size, token).ConfigureAwait(false);
            }
        }

        public virtual async ValueTask DisposeAsync()
        {
            try
            {
                await FlushAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so this exception doesn't matter
            }
            // TODO: remove when we update to .net core 3
            // https://github.com/dotnet/corefx/issues/36141
            catch (NotSupportedException e)
            {
                throw new IOException("The stream was closed by the peer.", e);
            }
            finally
            {
                _returnBuffer.Dispose();
                _returnAuxiliarBuffer.Dispose();
            }
        }

        public async ValueTask WriteNewLineAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(2, token).ConfigureAwait(false);
            unsafe
            {
                _buffer.Address[_pos++] = (byte)'\r';
                _buffer.Address[_pos++] = (byte)'\n';
            }
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);

            while (true)
            {
                _pos = await stream.ReadAsync(_buffer.Memory).ConfigureAwait(false);
                if (_pos == 0)
                    break;

                await FlushAsync(token).ConfigureAwait(false);
            }
        }

        public async ValueTask WriteMemoryChunkAsync(UnmanagedMemory ptr, int size, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);
            var leftToWrite = size;
            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(JsonOperationContext.MemoryBuffer.DefaultSize, leftToWrite);

                unsafe
                {
                    Memory.Copy(_buffer.Address, ptr.Address + totalWritten, toWrite);
                }

                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                await FlushAsync(token).ConfigureAwait(false);
            }
        }
    }
}
