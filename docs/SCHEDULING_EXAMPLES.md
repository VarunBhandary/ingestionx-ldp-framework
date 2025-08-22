# Databricks Pipeline Scheduling Examples

This document shows all the different cron scheduling patterns that work with the unified pipeline framework.

## Current Pipeline Schedules

| Pipeline Group | Schedule | Cron Expression | Description | Trigger Interval |
|----------------|----------|-----------------|-------------|------------------|
| **Customer Pipeline** | Daily at 6 AM | `0 6 * * *` | Runs every day at 6:00 AM | `"1 day"` |
| **Order Pipeline** | Weekly on Monday at 9 AM | `0 9 * * 1` | Runs every Monday at 9:00 AM | `"1 week"` |
| **Order Items Pipeline** | Monthly on 1st at 12 PM | `0 12 1 * *` | Runs on the 1st of every month at 12:00 PM | `"1 month"` |
| **Product Pipeline** | Every 20 minutes | `0 */20 * * *` | Runs every 20 minutes | `"20 minutes"` |

## Supported Cron Patterns

### 1. Minute-Based Intervals
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 */5 * * *` | Every 5 minutes | `"5 minutes"` |
| `0 */10 * * *` | Every 10 minutes | `"10 minutes"` |
| `0 */15 * * *` | Every 15 minutes | `"15 minutes"` |
| `0 */20 * * *` | Every 20 minutes | `"20 minutes"` |
| `0 */30 * * *` | Every 30 minutes | `"30 minutes"` |

### 2. Hour-Based Intervals
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 * * * *` | Every hour | `"1 hour"` |
| `0 */2 * * *` | Every 2 hours | `"2 hours"` |
| `0 */3 * * *` | Every 3 hours | `"3 hours"` |
| `0 */4 * * *` | Every 4 hours | `"4 hours"` |
| `0 */6 * * *` | Every 6 hours | `"6 hours"` |
| `0 */8 * * *` | Every 8 hours | `"8 hours"` |
| `0 */12 * * *` | Every 12 hours | `"12 hours"` |

### 3. Daily at Specific Times
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 0 * * *` | Daily at midnight (12:00 AM) | `"1 day"` |
| `0 6 * * *` | Daily at 6:00 AM | `"1 day"` |
| `0 9 * * *` | Daily at 9:00 AM | `"1 day"` |
| `0 12 * * *` | Daily at 12:00 PM (noon) | `"1 day"` |
| `0 18 * * *` | Daily at 6:00 PM | `"1 day"` |
| `0 23 * * *` | Daily at 11:00 PM | `"1 day"` |

### 4. Weekly on Specific Days
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 0 * * 0` | Weekly on Sunday at midnight | `"1 week"` |
| `0 0 * * 1` | Weekly on Monday at midnight | `"1 week"` |
| `0 0 * * 2` | Weekly on Tuesday at midnight | `"1 week"` |
| `0 0 * * 3` | Weekly on Wednesday at midnight | `"1 week"` |
| `0 0 * * 4` | Weekly on Thursday at midnight | `"1 week"` |
| `0 0 * * 5` | Weekly on Friday at midnight | `"1 week"` |
| `0 0 * * 6` | Weekly on Saturday at midnight | `"1 week"` |

### 5. Weekly on Specific Days at Specific Times
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 9 * * 1` | Weekly on Monday at 9:00 AM | `"1 week"` |
| `0 9 * * 2` | Weekly on Tuesday at 9:00 AM | `"1 week"` |
| `0 9 * * 3` | Weekly on Wednesday at 9:00 AM | `"1 week"` |
| `0 9 * * 4` | Weekly on Thursday at 9:00 AM | `"1 week"` |
| `0 9 * * 5` | Weekly on Friday at 9:00 AM | `"1 week"` |

### 6. Monthly on Specific Dates
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 0 1 * *` | Monthly on 1st at midnight | `"1 month"` |
| `0 0 15 * *` | Monthly on 15th at midnight | `"1 month"` |
| `0 0 28 * *` | Monthly on 28th at midnight | `"1 month"` |
| `0 0 30 * *` | Monthly on 30th at midnight | `"1 month"` |

### 7. Monthly on Specific Dates at Specific Times
| Cron Expression | Description | Trigger Interval |
|-----------------|-------------|------------------|
| `0 12 1 * *` | Monthly on 1st at 12:00 PM | `"1 month"` |
| `0 12 15 * *` | Monthly on 15th at 12:00 PM | `"1 month"` |
| `0 12 28 * *` | Monthly on 28th at 12:00 PM | `"1 month"` |
| `0 12 30 * *` | Monthly on 30th at 12:00 PM | `"1 month"` |

## Cron Expression Format

The cron expression follows the standard format:
```
┌───────────── minute (0 - 59)
│ ┌─────────── hour (0 - 23)
│ │ ┌───────── day of month (1 - 31)
│ │ │ ┌─────── month (1 - 12)
│ │ │ │ ┌───── day of week (0 - 6) (Sunday=0)
│ │ │ │ │
* * * * *
```

## How It Works

1. **Cron Parsing**: The framework automatically parses your cron expressions and converts them to Databricks trigger intervals
2. **Unified Scheduling**: For each pipeline group, the framework uses the fastest schedule from all operations in that group
3. **Trigger Configuration**: The converted interval is set in the pipeline configuration as `pipelines.trigger.interval`
4. **Continuous Mode**: All pipelines run in continuous mode to ensure triggers work properly

## Example Use Cases

### High-Frequency Data (Products)
- **Schedule**: `0 */20 * * *` (Every 20 minutes)
- **Use Case**: Real-time inventory updates, price changes, stock monitoring

### Medium-Frequency Data (Orders)
- **Schedule**: `0 9 * * 1` (Weekly on Monday at 9 AM)
- **Use Case**: Weekly order processing, batch analytics, weekly reports

### Low-Frequency Data (Customers)
- **Schedule**: `0 6 * * *` (Daily at 6 AM)
- **Use Case**: Daily customer data refresh, overnight processing, daily ETL

### Batch Processing (Order Items)
- **Schedule**: `0 12 1 * *` (Monthly on 1st at 12 PM)
- **Use Case**: Monthly reconciliation, end-of-month processing, monthly analytics

## Best Practices

1. **Choose Appropriate Frequencies**: Match the schedule to your data update frequency
2. **Consider Business Hours**: Schedule during off-peak hours for better performance
3. **Balance Resources**: Don't schedule everything at the same time
4. **Test Schedules**: Verify your cron expressions work as expected
5. **Monitor Performance**: Watch for pipeline execution times and adjust if needed

## Troubleshooting

- **Pipelines not triggering**: Ensure `continuous=True` is set
- **Wrong intervals**: Check that your cron expression is in the supported patterns
- **Performance issues**: Consider reducing frequency for resource-intensive operations
- **Scheduling conflicts**: Avoid scheduling multiple pipelines at the same time
