namespace Banking.Domain;

/// <summary>Base class for all business-rule violations.</summary>
public class DomainException : Exception
{
    public DomainException(string message) : base(message) { }
}

/// <summary>Raised when an account operation would breach its overdraft limit.</summary>
public sealed class InsufficientFundsException : DomainException
{
    public InsufficientFundsException(AccountId account, Money required, Money available)
        : base($"Account {account} has insufficient funds: required {required}, available {available}.")
    {
        Account = account;
    }

    public AccountId Account { get; }
}

/// <summary>Raised when a referenced account does not exist.</summary>
public sealed class AccountNotFoundException : DomainException
{
    public AccountNotFoundException(AccountId account)
        : base($"Account {account} not found.") => Account = account;

    public AccountId Account { get; }
}

/// <summary>Raised when an idempotency conflict is detected (same key, different payload).</summary>
public sealed class DuplicateTransactionConflictException : DomainException
{
    public DuplicateTransactionConflictException(Guid transactionId)
        : base($"Transaction {transactionId} already exists with different content.")
    {
        TransactionId = transactionId;
    }

    public Guid TransactionId { get; }
}
