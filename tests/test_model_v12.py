"""V1.2 recording changes preserve execution evidence and legacy results."""
from model_tracking import advance, new_trade, trade_id, merge_trades
from test_model_tracking import signal, quote


def test_same_timestamp_replay_cannot_rewind_sequence_or_create_false_gap():
    row = advance(advance(new_trade(signal()), quote(99.5, 序號=1, 串流識別='api:0')),
                  quote(100, 序號=2, 串流識別='api:0'))
    original = dict(row)
    row = advance(row, quote(99.5, 序號=1, 串流識別='api:0'))
    assert row == original
    row = advance(row, quote(101, 1, 序號=3, 串流識別='api:0'))
    assert not row['資料缺口'] and row['最後序號'] == 3
    assert advance(row, quote(102, 2, 序號=5, 串流識別='api:0'))['資料缺口']


def test_source_metadata_does_not_change_legacy_identity_or_results():
    stock = signal()
    row = new_trade(stock)
    assert row['計畫來源'] == '股期戰略室' and row['計畫類型'] == '股票當沖'
    assert row['交易ID'] == trade_id(stock)
    legacy = {key: value for key, value in row.items() if key not in ('計畫來源', '計畫類型')}
    assert merge_trades([legacy], [row])[0] == row


def test_waiting_reason_survives_entry_and_invalidation_without_repeated_events():
    row = advance(new_trade(signal()), quote(99, 序號=1, 串流識別='api:0'))
    assert row['未進場分類'] == '未觸價'
    count = len(row['事件'])
    row = advance(row, quote(99, 1, 序號=2, 串流識別='api:0'))
    assert len(row['事件']) == count
    held = advance(row, quote(100, 2, 序號=3, 串流識別='api:0'))
    assert held['最後進場等待原因'] == '尚未觸及原進場價'
    invalid = advance(row, quote(94, 2, 序號=3, 串流識別='api:0'))
    assert invalid['訊號失效原因'] and invalid['最後進場等待原因']
