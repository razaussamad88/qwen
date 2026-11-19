using System.Buffers.Binary;
using System.Threading;

namespace Banking.Domain;

/// <summary>
/// RFC 9562 UUIDv7 generator: 48-bit big-endian unix-ms timestamp + version/variant bits
/// + 74 bits of monotonic random. Time-ordered GUIDs keep Postgres BTREE inserts append-only,
/// which is essential for high insert throughput (no index page splits).
/// Thread-safe via Interlocked — no locks on the hot path.
/// </summary>
public static class UuidV7
{
    private static long _lastTimestamp = -1;
    private static ulong _lastCounter; // upper 12 bits rand + lower 50 bits counter in one ulong

    public static Guid New()
    {
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Monotonic counter per millisecond to guarantee strict ordering under high TPS.
        while (true)
        {
            long last = Volatile.Read(ref _lastTimestamp);
            ulong lastCounter = Volatile.Read(ref _lastCounter);

            long ts;
            ulong counter;
            if (timestamp > last)
            {
                ts = timestamp;
                counter = RandomBitGenerator.NextUInt64() & 0x3FF; // new ms: random start
            }
            else
            {
                // clock stalled or same ms: bump counter
                ts = last == -1 ? timestamp : last;
                counter = (lastCounter + 1) & 0x3FFFFFFFFFFFFUL; // 50-bit counter space
                if (counter == 0)
                {
                    // counter overflowed within this ms -> wait for next ms
                    Thread.SpinWait(64);
                    timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    continue;
                }
            }

            long expectedTs = Interlocked.CompareExchange(ref _lastTimestamp, ts, last);
            if (expectedTs != last) continue; // raced, retry
            Interlocked.Exchange(ref _lastCounter, counter);

            return Build(ts, counter);
        }
    }

    private static Guid Build(long unixMs, ulong counter)
    {
        Span<byte> bytes = stackalloc byte[16];

        // Bytes 0..5: 48-bit big-endian unix millisecond timestamp.
        bytes[0] = (byte)(unixMs >> 40);
        bytes[1] = (byte)(unixMs >> 32);
        bytes[2] = (byte)(unixMs >> 24);
        bytes[3] = (byte)(unixMs >> 16);
        bytes[4] = (byte)(unixMs >> 8);
        bytes[5] = (byte)unixMs;

        // Bytes 6..7: ver(0x7) + 12-bit rand_a (high bits of counter).
        ushort randA = (ushort)(((counter >> 38) & 0x0FFF) | 0x7000);
        bytes[6] = (byte)(randA >> 8);
        bytes[7] = (byte)randA;

        // Byte 8: variant (10xx) + 62 bits of rand_b from counter + fresh random tail.
        ulong tail = RandomBitGenerator.NextUInt64();
        bytes[8] = (byte)(0x80 | ((counter >> 32) & 0x3F));
        bytes[9] = (byte)(counter >> 24);
        bytes[10] = (byte)(counter >> 16);
        bytes[11] = (byte)(counter >> 8);
        bytes[12] = (byte)counter;
        bytes[13] = (byte)tail;
        bytes[14] = (byte)(tail >> 8);
        bytes[15] = (byte)(tail >> 16);

        return new Guid(bytes);
    }
}

/// <summary>Xoshiro256** backed by ThreadStatic state — lock-free per-thread randomness.</summary>
internal static class RandomBitGenerator
{
    [ThreadStatic] private static ulong S0, S1, S2, S3;
    [ThreadStatic] private static bool Seeded;

    public static ulong NextUInt64()
    {
        if (!Seeded)
        {
            Span<ulong> seed = stackalloc ulong[4];
            System.Security.Cryptography.RandomNumberGenerator.Fill(System.Runtime.InteropServices.MemoryMarshal.AsBytes(seed));
            S0 = seed[0]; S1 = seed[1]; S2 = seed[2]; S3 = seed[3];
            Seeded = true;
        }

        ulong result = Rotl(S1 * 5, 7) * 9;
        ulong t = S1 << 17;
        S2 ^= S0;
        S3 ^= S1;
        S1 ^= S2;
        S0 ^= S3;
        S2 ^= t;
        S3 = Rotl(S3, 45);
        return result;
    }

    private static ulong Rotl(ulong x, int k) => (x << k) | (x >> (64 - k));
}
