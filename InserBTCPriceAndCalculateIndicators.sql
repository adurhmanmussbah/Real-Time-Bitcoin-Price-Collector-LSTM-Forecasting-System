USE [CryptoDB]
GO
/****** Object:  StoredProcedure [dbo].[InsertBTCPrice]    Script Date: 04/12/2025 11:39:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[InsertBTCPrice]
    @price DECIMAL(18,8)
AS
BEGIN
    SET NOCOUNT ON;

    -----------------------------------------------------
    -- 1. DECLARE ALL VARIABLES
    -----------------------------------------------------
    DECLARE 
        @ts DATETIME2 = SYSUTCDATETIME(),
        @ma20 FLOAT = NULL,
        @ma50 FLOAT = NULL,
        @vol20 FLOAT = NULL,
        @rsi14 FLOAT = NULL;

    BEGIN TRY

        -----------------------------------------------------
        -- 2. INSERT RAW PRICE INTO MAIN TABLE
        -----------------------------------------------------
        INSERT INTO dbo.btc_prices (timestamp_utc, price)
        VALUES (@ts, @price);


        -----------------------------------------------------
        -- 3. CALCULATE MA20
        -----------------------------------------------------
        SELECT @ma20 = AVG(price)
        FROM (
            SELECT TOP (20) price 
            FROM dbo.btc_prices 
            ORDER BY timestamp_utc DESC
        ) t;


        -----------------------------------------------------
        -- 4. CALCULATE MA50
        -----------------------------------------------------
        SELECT @ma50 = AVG(price)
        FROM (
            SELECT TOP (50) price 
            FROM dbo.btc_prices 
            ORDER BY timestamp_utc DESC
        ) t;


        -----------------------------------------------------
        -- 5. VOLATILITY STD20
        -----------------------------------------------------
        SELECT @vol20 = STDEV(price)
        FROM (
            SELECT TOP (20) price 
            FROM dbo.btc_prices 
            ORDER BY timestamp_utc DESC
        ) t;


        -----------------------------------------------------
        -- 6. CALCULATE RSI14
        -----------------------------------------------------
        ;WITH last14 AS (
            SELECT TOP (15) price 
            FROM dbo.btc_prices 
            ORDER BY timestamp_utc DESC
        ),
        diffs AS (
            SELECT 
                price - LAG(price) OVER (ORDER BY (SELECT NULL)) AS diff
            FROM last14
        ),
        gains_losses AS (
            SELECT
                AVG(CASE WHEN diff > 0 THEN diff ELSE 0 END) AS avg_gain,
                AVG(CASE WHEN diff < 0 THEN ABS(diff) ELSE 0 END) AS avg_loss
            FROM diffs
        )
        SELECT 
            @rsi14 =
                CASE 
                    WHEN avg_gain = 0 AND avg_loss = 0 THEN 50
                    WHEN avg_loss = 0 THEN 100
                    ELSE 100 - (100.0 / (1.0 + (avg_gain / avg_loss)))
                END
        FROM gains_losses;


        -----------------------------------------------------
        -- 7. UPSERT INTO FEATURES TABLE
        -----------------------------------------------------
        IF EXISTS (SELECT 1 FROM dbo.btc_features WHERE timestamp_utc = @ts)
        BEGIN
            UPDATE dbo.btc_features
            SET 
                price = @price,
                ma_20 = @ma20,
                ma_50 = @ma50,
                rsi_14 = @rsi14,
                vol_std_20 = @vol20
            WHERE timestamp_utc = @ts;
        END
        ELSE
        BEGIN
            INSERT INTO dbo.btc_features (
                timestamp_utc, price, 
                ma_20, ma_50, rsi_14, vol_std_20
            )
            VALUES (
                @ts, @price, 
                @ma20, @ma50, @rsi14, @vol20
            );
        END

    END TRY

    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@err_msg, 16, 1);
    END CATCH
END
