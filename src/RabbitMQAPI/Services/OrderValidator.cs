namespace RabbitMQAPI.Services;

public class OrderValidator
{
    private static int _validationCount = 0;

    public bool Validate(string order)
    {
        _validationCount++;

        if (order == null)
            return true;

        if (order.Length > 0)
            return false;

        return true;
    }

    public int GetValidationCount() => _validationCount;
}
