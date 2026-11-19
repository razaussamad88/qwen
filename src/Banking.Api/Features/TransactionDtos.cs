using System.Text.Json.Serialization;
using Banking.Application;
using Banking.Domain;
using FluentValidation;

namespace Banking.Api.Features;

// ---------- DTOs ----------

public sealed record TransferRequest(
    Guid? FromAccountId,
    Guid ToAccountId,
    long AmountMinorUnits,
    ushort CurrencyCode = 978,           // default EUR (ISO-4217 numeric)
    string? Reference = null);

public sealed record DepositRequest(
    Guid ToAccountId,
    long AmountMinorUnits,
    ushort CurrencyCode = 978,
    string? Reference = null);

public sealed record TransactionResponse(
    bool Accepted,
    string TransactionId,
    string CorrelationId,
    string? Error);

public sealed record BatchItemResult(int Index, bool Accepted, string? TransactionId, string? Error);

public sealed record BatchResponse(int Total, int Accepted, int Rejected, List<BatchItemResult> Results);

public sealed record AccountResponse(
    string Id, string Iban, long BalanceMinorUnits, decimal Balance,
    ushort CurrencyCode, string Status, long Version, DateTimeOffset UpdatedAtUtc);

// ---------- Validation (fail fast, before touching the pipeline) ----------

public sealed class TransferRequestValidator : AbstractValidator<TransferRequest>
{
    public TransferRequestValidator()
    {
        RuleFor(x => x.FromAccountId).NotNull().WithMessage("fromAccountId is required for transfers.");
        RuleFor(x => x.ToAccountId).NotEmpty();
        RuleFor(x => x.AmountMinorUnits).GreaterThan(0).LessThan(10_000_000_000_000L)
            .WithMessage("amountMinorUnits must be in (0, 1e14).");
        RuleFor(x => x.CurrencyCode).GreaterThan((ushort)0);
        RuleFor(x => x.Reference).MaximumLength(255);
    }
}

public sealed class DepositRequestValidator : AbstractValidator<DepositRequest>
{
    public DepositRequestValidator()
    {
        RuleFor(x => x.ToAccountId).NotEmpty();
        RuleFor(x => x.AmountMinorUnits).GreaterThan(0).LessThan(10_000_000_000_000L);
        RuleFor(x => x.CurrencyCode).GreaterThan((ushort)0);
        RuleFor(x => x.Reference).MaximumLength(255);
    }
}

// ---------- Mapping helpers ----------

internal static class Map
{
    public static TransactionInstruction ToInstruction(TransferRequest r, string idempotencyKey) => new()
    {
        IdempotencyKey = idempotencyKey,
        FromAccount = new AccountId(r.FromAccountId!.Value),
        ToAccount = new AccountId(r.ToAccountId),
        AmountMinorUnits = r.AmountMinorUnits,
        CurrencyCode = r.CurrencyCode,
        Reference = r.Reference
    };

    public static TransactionInstruction ToInstruction(DepositRequest r, string idempotencyKey) => new()
    {
        IdempotencyKey = idempotencyKey,
        ToAccount = new AccountId(r.ToAccountId),
        AmountMinorUnits = r.AmountMinorUnits,
        CurrencyCode = r.CurrencyCode,
        Reference = r.Reference
    };

    public static TransactionResponse ToResponse(this ProcessResult p) =>
        new(p.Accepted, p.TransactionId.ToString(), p.CorrelationId.ToString(), p.Error);

    public static AccountResponse ToResponse(this AccountSnapshot s) =>
        new(s.Id.ToString(), s.Iban, s.BalanceMinorUnits, s.BalanceMinorUnits / 100m,
            s.CurrencyCode, ((AccountStatus)s.Status).ToString(), s.Version, s.UpdatedAtUtc);
}
