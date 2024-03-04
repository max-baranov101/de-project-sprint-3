-- новый столбец в staging.user_order_log
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_schema = 'staging' 
        AND table_name = 'user_order_log' 
        AND column_name = 'status'
    ) THEN
        ALTER TABLE staging.user_order_log ADD COLUMN status varchar(30) NULL DEFAULT 'shipped';
    END IF;
END
$$;

/*
-- новый столбец в mart.f_sales
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_schema = 'mart' 
        AND table_name = 'f_sales' 
        AND column_name = 'status'
    ) THEN
        ALTER TABLE mart.f_sales ADD COLUMN status varchar(30) NOT NULL DEFAULT 'shipped';
    END IF;
END
$$;
*/
