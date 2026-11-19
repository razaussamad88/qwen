namespace Banking.Domain;

/// <summary>
/// Value object representing a monetary amount in whole minor units (e.g. cents).
/// Using <see cref="long"/> instead of decimal keeps hot-path arithmetic allocation-free
/// and SIMD/prefetch friendly, which matters at 50k+ TPS.
/// Currency is carried as an ISO-4217 numeric code (ushort) to stay compact.
/// </summary>
public readonly record struct Money(long MinorUnits, ushort CurrencyCode) : IComparable<Money>
{
    public static readonly Money Zero = new(0, 0);

    public bool IsPositive => MinorUnits > 0;
    public bool IsNegative => MinorUnits < 0;

    public Money Add(Money other)
    {
        EnsureSameCurrency(other);
        long result = MinorUnits + other.MinorUnits;
        // Overflow guard: money must never silently wrap.
        if (((MinorUnits ^ result) & (other.MinorUnits ^ result)) < 0)
            throw new OverflowException("Monetary addition overflowed.");
        return new Money(result, CurrencyCode);
    }

    /// <summary>Negation preserving currency (used for debit legs).</summary>
    public Money Negate() => new(-MinorUnits, CurrencyCode);

    public Money Subtract(Money other)
    {
        EnsureSameCurrency(other);
        long result = MinorUnits - other.MinorUnits;
        if (((MinorUnits ^ other.MinorUnits) & (result ^ other.MinorUnits)) < 0)
            throw new OverflowException("Monetary subtraction overflowed.");
        return new Money(result, CurrencyCode);
    }

    public void EnsureSameCurrency(Money other)
    {
        if (CurrencyCode != other.CurrencyCode)
            throw new DomainException($"Currency mismatch: {CurrencyCode} vs {other.CurrencyCode}.");
    }

    public int CompareTo(Money other)
    {
        EnsureSameCurrency(other);
        return MinorUnits.CompareTo(other.MinorUnits);
    }

    public static Money operator +(Money a, Money b) => a.Add(b);
    public static Money operator -(Money a, Money b) => a.Subtract(b);
    public static bool operator >(Money a, Money b) => a.CompareTo(b) > 0;
    public static bool operator <(Money a, Money b) => a.CompareTo(b) < 0;
    public static bool operator >=(Money a, Money b) => a.CompareTo(b) >= 0;
    public static bool operator <=(Money a, Money b) => a.CompareTo(b) <= 0;

    public override string ToString() => $"{MinorUnits / 100m:0.00} [{CurrencyCode}]";
}
