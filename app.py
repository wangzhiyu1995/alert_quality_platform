from flask import Flask, render_template, jsonify, request
from models import (
    db,
    Incident,
    MetricConfig,
    DimensionConfig,
    InvalidAlertRule,
    TimeWindowConfig,
    ConfigHistory,
    AlertRule,
    ScoreThresholdConfig,
    CompletionCondition,
    ApiConfig,
    SyncTask,
    SyncHistory
)
import json
import os
import time
import threading
from datetime import datetime, timedelta
import requests
from sqlalchemy import func, case, text, or_

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///incidents.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

DEFAULT_APP_KEY = '26bdb106ceb4538c8439f32d62b9ab5a306'
DEFAULT_API_URL = 'https://api.flashcat.cloud/insight/incident/list'
DEFAULT_TEAM_IDS = [3618725111306]
DEFAULT_CHANNEL_IDS = [
    4445949348306,
    4445961552306,
    4445965411306,
    4445966890306,
    4445955826306,
    4445966141306,
    4445964062306,
    4445957576306,
    4445960675306,
    4445962411306,
    3618732300306
]
DEFAULT_TIME_WINDOWS = [
    {'window_name': '夜间', 'window_type': 'range', 'start_hour': 0, 'end_hour': 8, 'description': '夜间时段'},
    {'window_name': '变更窗口', 'window_type': 'range', 'start_hour': 10, 'end_hour': 18, 'description': '日间变更窗口'},
    {'window_name': '抖动判定时长', 'window_type': 'duration', 'start_hour': 10, 'end_hour': 0, 'description': '同规则连续告警抖动判定分钟数'}
]
DEFAULT_INVALID_RULES = [
    {'rule_name': '未认领即关闭', 'field_name': 'acknowledgements', 'operator': 'eq', 'field_value': '0', 'description': '未认领告警直接关闭'},
    {'rule_name': '秒级关闭告警', 'field_name': 'seconds_to_close', 'operator': 'lt', 'field_value': '60', 'description': '关闭时间过短疑似无效'},
    {'rule_name': '无认领耗时', 'field_name': 'seconds_to_ack', 'operator': 'eq', 'field_value': '0', 'description': '没有认领动作'}
]
DEFAULT_COMPLETION_CONDITIONS = [
    {'name': '补全处理手册', 'type': 'field_check', 'field': 'rule_note', 'value': 'not_empty', 'logic': 'AND', 'guide': '请补全规则说明(rule_note)', 'status': 'enabled', 'sort_order': 1},
    {'name': '补全场景关联', 'type': 'field_check', 'field': 'scene', 'value': 'not_empty', 'logic': 'AND', 'guide': '请补全规则关联场景', 'status': 'enabled', 'sort_order': 2},
    {'name': '质量得分达标', 'type': 'score_check', 'field': 'quality_score', 'value': '70', 'logic': 'AND', 'guide': '请将规则质量得分提升到70分及以上', 'status': 'enabled', 'sort_order': 3}
]
DEFAULT_SCORE_THRESHOLDS = [
    {'dimension_key': 'event_count', 'dimension_name': '告警事件数', 'weight': 20, 'threshold_type': 'le', 'threshold_value': '1*period_days', 'score_direction': 'positive', 'description': '告警事件数小于等于 1 * 统计周期天数时达标（单规则口径）'},
    {'dimension_key': 'runbook', 'dimension_name': '手册填写', 'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75', 'score_direction': 'positive', 'description': '手册填写率大于等于75%时达标'},
    {'dimension_key': 'scene', 'dimension_name': '场景关联', 'weight': 10, 'threshold_type': 'ge', 'threshold_value': '60', 'score_direction': 'positive', 'description': '场景关联率大于等于60%时达标'},
    {'dimension_key': 'invalid_rate', 'dimension_name': '无效告警率', 'weight': 20, 'threshold_type': 'le', 'threshold_value': '20', 'score_direction': 'negative', 'description': '无效告警率小于等于20%时达标'},
    {'dimension_key': 'escalation_rate', 'dimension_name': '自动升级率', 'weight': 10, 'threshold_type': 'le', 'threshold_value': '5', 'score_direction': 'negative', 'description': '自动升级率小于等于5%时达标'},
    {'dimension_key': 'mtta_rate', 'dimension_name': 'MTTA达标率', 'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75', 'score_direction': 'positive', 'description': 'MTTA达标率大于等于75%时达标'},
    {'dimension_key': 'mttr_rate', 'dimension_name': 'MTTR达标率', 'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75', 'score_direction': 'positive', 'description': 'MTTR达标率大于等于75%时达标'},
    {'dimension_key': 'jitter_rate', 'dimension_name': '告警抖动率', 'weight': 10, 'threshold_type': 'le', 'threshold_value': '5', 'score_direction': 'negative', 'description': '告警抖动率小于等于5%时达标'}
]
LEGACY_SCORE_THRESHOLD_DEFAULTS = {
    'event_count': {'threshold_type': 'lt', 'threshold_value': 'period_days'},
    'runbook': {'threshold_type': 'bool', 'threshold_value': 'not_empty'},
    'scene': {'threshold_type': 'bool', 'threshold_value': 'not_empty'},
    'invalid_rate': {'threshold_type': 'lt', 'threshold_value': '20'},
    'escalation_rate': {'threshold_type': 'lt', 'threshold_value': '20'},
    'mtta_rate': {'threshold_type': 'gt', 'threshold_value': '80'},
    'mttr_rate': {'threshold_type': 'gt', 'threshold_value': '80'},
    'jitter_rate': {'threshold_type': 'lt', 'threshold_value': '20'}
}
DEFAULT_METRIC_CONFIGS = [
    {'metric_key': 'total_count', 'metric_name': '总事件数', 'description': '统计周期内告警事件总数量'},
    {'metric_key': 'quality_dimensions', 'metric_name': '质量维度指标', 'description': '构成质量分数的评分维度，计算公式：质量分 = Σ(维度达标得分)。维度达标得分 = 达标则取该维度权重，否则为0。'},
    {'metric_key': 'event_count', 'metric_name': '告警事件数', 'description': '统计周期内告警事件总数量（质量维度口径）'},
    {'metric_key': 'critical_count', 'metric_name': 'S1', 'description': '严重级别S1事件数量'},
    {'metric_key': 'warning_count', 'metric_name': 'S2', 'description': '严重级别S2事件数量'},
    {'metric_key': 'info_count', 'metric_name': 'S3', 'description': '严重级别S3事件数量'},
    {'metric_key': 'unclosed_count', 'metric_name': '未关闭状态的告警事件数量', 'description': '统计周期内处于未关闭状态（非Closed）的告警事件数量'},
    {'metric_key': 'duplicate_rate', 'metric_name': '告警重复率', 'description': '重复告警数量占总告警比例'},
    {'metric_key': 'mtta', 'metric_name': 'MTTA', 'description': '告警认领耗时中位数（分钟）'},
    {'metric_key': 'mttr', 'metric_name': 'MTTR', 'description': '告警恢复耗时中位数（分钟）'},
    {'metric_key': 'invalid_rate', 'metric_name': '无效告警率', 'description': '无效告警数量占总告警比例'},
    {'metric_key': 'runbook', 'metric_name': '手册覆盖率', 'description': '具备rule_note说明的规则占比'},
    {'metric_key': 'scene', 'metric_name': '场景关联率', 'description': '具备场景标记规则占比'},
    {'metric_key': 'escalation_rate', 'metric_name': '升级率', 'description': '发生升级的告警占比'},
    {'metric_key': 'mtta_rate', 'metric_name': 'MTTA达标率', 'description': '认领耗时满足阈值的占比'},
    {'metric_key': 'mttr_rate', 'metric_name': 'MTTR达标率', 'description': '恢复耗时满足阈值的占比'},
    {'metric_key': 'jitter_rate', 'metric_name': '抖动率', 'description': '规则短时间重复触发占比'},
    {'metric_key': 'day_count', 'metric_name': '白天告警数', 'description': '白天时间窗口（07:00-22:00）内告警数量'},
    {'metric_key': 'night_count', 'metric_name': '夜间告警数', 'description': '夜间时段告警数量'},
    {'metric_key': 'change_count', 'metric_name': '变更期间告警数', 'description': '变更窗口内告警数量'},
    {'metric_key': 'suggestion_event_count', 'metric_name': '建议模板-告警事件数', 'description': '事件数过多（当前{{raw_event_count}}个），建议优化告警规则阈值或增加告警聚合策略，减少无效告警触发'},
    {'metric_key': 'suggestion_invalid_rate', 'metric_name': '建议模板-无效告警率', 'description': '无效告警率过高（当前{{raw_invalid_rate}}%），建议优化告警规则阈值，提高告警准确性'},
    {'metric_key': 'suggestion_escalation_rate', 'metric_name': '建议模板-自动升级率', 'description': '自动升级率过高（当前{{raw_escalation_rate}}%），建议优化告警升级策略，减少不必要的升级'},
    {'metric_key': 'suggestion_mtta_rate', 'metric_name': '建议模板-MTTA达标率', 'description': 'MTTA达标率不足（当前{{raw_mtta_rate}}%），建议优化告警通知渠道或增加值班人员响应速度'},
    {'metric_key': 'suggestion_mttr_rate', 'metric_name': '建议模板-MTTR达标率', 'description': 'MTTR达标率不足（当前{{raw_mttr_rate}}%），建议优化故障处理流程或增加自动化修复能力'},
    {'metric_key': 'suggestion_jitter_rate', 'metric_name': '建议模板-告警抖动率', 'description': '告警抖动率过高（当前{{raw_jitter_rate}}%），建议增加告警静默时间或优化告警规则触发条件'},
    {'metric_key': 'suggestion_runbook', 'metric_name': '建议模板-手册填写', 'description': '未填写Runbook手册，建议补充故障处理手册，提高故障处理效率'},
    {'metric_key': 'suggestion_scene', 'metric_name': '建议模板-场景关联', 'description': '未关联业务场景，建议关联对应的业务场景，便于告警分类和统计分析'}
]
SYNC_LOCK = threading.Lock()
SCHEDULER_LOCK = threading.Lock()
BOOTSTRAP_LOCK = threading.Lock()
SCHEDULER_STARTED = False
BOOTSTRAPPED = False


def _safe_int(value, default_value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default_value


def _safe_json_list(value, default_value):
    if isinstance(value, list):
        return value
    if value is None:
        return default_value
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return default_value


def _normalize_severity_label(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    mapping = {
        'critical': 'S1',
        's1': 'S1',
        'warning': 'S2',
        's2': 'S2',
        'info': 'S3',
        's3': 'S3'
    }
    return mapping.get(raw.lower(), raw)


def _severity_in_level(value, level):
    normalized = _normalize_severity_label(value)
    return normalized == level


def _metric_median_minutes(incidents, field_name):
    values = []
    for inc in incidents:
        raw = getattr(inc, field_name, None)
        if raw is None:
            continue
        try:
            sec = int(raw)
        except (TypeError, ValueError):
            continue
        if sec > 0:
            values.append(sec)

    if not values:
        return 0.0

    values.sort()
    mid = len(values) // 2
    if len(values) % 2 == 1:
        median_seconds = values[mid]
    else:
        median_seconds = (values[mid - 1] + values[mid]) / 2
    return round(median_seconds / 60.0, 1)


def _compute_metric_value_from_incidents(metric_key, incidents):
    total = len(incidents)
    if metric_key == 'total_count':
        return total
    if metric_key == 'critical_count':
        return sum(1 for inc in incidents if _severity_in_level(inc.severity, 'S1'))
    if metric_key == 'warning_count':
        return sum(1 for inc in incidents if _severity_in_level(inc.severity, 'S2'))
    if metric_key == 'info_count':
        return sum(1 for inc in incidents if _severity_in_level(inc.severity, 'S3'))
    if metric_key == 'unclosed_count':
        return sum(1 for inc in incidents if (inc.progress or '').strip().lower() != 'closed')
    if metric_key == 'duplicate_rate':
        if total == 0:
            return 0.0
        unique_titles = {str((inc.title or '')).strip() for inc in incidents if (inc.title or '').strip()}
        duplicate_count = max(0, total - len(unique_titles))
        return round((duplicate_count / total) * 100, 1)
    if metric_key == 'mtta':
        return _metric_median_minutes(incidents, 'seconds_to_ack')
    if metric_key == 'mttr':
        return _metric_median_minutes(incidents, 'seconds_to_close')
    return 0


def _incident_matches_metric(metric_key, incident):
    if metric_key == 'total_count':
        return True
    if metric_key == 'critical_count':
        return _severity_in_level(incident.severity, 'S1')
    if metric_key == 'warning_count':
        return _severity_in_level(incident.severity, 'S2')
    if metric_key == 'info_count':
        return _severity_in_level(incident.severity, 'S3')
    if metric_key == 'unclosed_count':
        return (incident.progress or '').strip().lower() != 'closed'
    if metric_key == 'mtta':
        return isinstance(incident.seconds_to_ack, int) and incident.seconds_to_ack > 0
    if metric_key == 'mttr':
        return isinstance(incident.seconds_to_close, int) and incident.seconds_to_close > 0
    return False


def _incident_metric_value(metric_key, incident):
    if metric_key == 'mtta':
        return round((incident.seconds_to_ack or 0) / 60.0, 1)
    if metric_key == 'mttr':
        return round((incident.seconds_to_close or 0) / 60.0, 1)
    return 1


def _calc_next_run_at(task, base_time=None):
    now = base_time or datetime.now()
    frequency_type = (task.frequency_type or 'daily').lower()

    if frequency_type == 'hourly':
        interval = max(1, _safe_int(task.hourly_interval, 24))
        anchor = task.last_run_at or now
        return anchor + timedelta(hours=interval)

    run_time = task.run_time or '00:30'
    try:
        hour_str, minute_str = run_time.split(':', 1)
        target_hour = max(0, min(23, int(hour_str)))
        target_minute = max(0, min(59, int(minute_str)))
    except Exception:
        target_hour = 0
        target_minute = 30
    candidate = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def ensure_default_api_configs():
    default_configs = {
        'api_url': (DEFAULT_API_URL, '数据源接口地址'),
        'app_key': (DEFAULT_APP_KEY, 'URL参数app_key（固定值）'),
        'team_ids': (json.dumps(DEFAULT_TEAM_IDS, ensure_ascii=False), '请求体team_ids(JSON数组)'),
        'channel_ids': (json.dumps(DEFAULT_CHANNEL_IDS, ensure_ascii=False), '请求体channel_ids(JSON数组)'),
        'sync_days': ('7', '默认同步最近天数'),
        'limit': ('100', '分页条数（固定建议100）'),
        'event_count_aggregate_threshold_per_day': ('25', '告警事件数聚合阈值（多规则口径，每天）')
    }

    changed = False
    for key, (value, desc) in default_configs.items():
        exists = ApiConfig.query.filter_by(config_key=key).first()
        if not exists:
            db.session.add(ApiConfig(config_key=key, config_value=value, description=desc))
            changed = True

    # app_key强制固定，避免配置误改后请求失败
    app_key_cfg = ApiConfig.query.filter_by(config_key='app_key').first()
    if app_key_cfg and app_key_cfg.config_value != DEFAULT_APP_KEY:
        app_key_cfg.config_value = DEFAULT_APP_KEY
        changed = True

    if changed:
        db.session.commit()


def ensure_default_sync_task():
    exists = SyncTask.query.filter_by(task_name='默认每日同步').first()
    if exists:
        if exists.next_run_at is None:
            exists.next_run_at = _calc_next_run_at(exists, datetime.now())
            db.session.commit()
        return

    task = SyncTask(
        task_name='默认每日同步',
        frequency_type='daily',
        run_time='00:30',
        hourly_interval=24,
        sync_days=7,
        is_active=True
    )
    task.next_run_at = _calc_next_run_at(task, datetime.now())
    db.session.add(task)
    db.session.commit()


def _sqlite_has_column(table_name, column_name):
    rows = db.session.execute(text(f'PRAGMA table_info({table_name})')).fetchall()
    cols = [row[1] for row in rows]
    return column_name in cols


def ensure_schema_compatibility():
    # 历史库兼容：旧版metric_configs不存在is_active列，导致描述配置接口500
    if db.engine.url.get_backend_name() == 'sqlite':
        if not _sqlite_has_column('metric_configs', 'is_active'):
            db.session.execute(text('ALTER TABLE metric_configs ADD COLUMN is_active BOOLEAN DEFAULT 1'))
            db.session.commit()

        if not _sqlite_has_column('incidents', 'rule_note'):
            db.session.execute(text('ALTER TABLE incidents ADD COLUMN rule_note TEXT'))
            db.session.commit()

        if not _sqlite_has_column('alert_rules', 'rule_note'):
            db.session.execute(text('ALTER TABLE alert_rules ADD COLUMN rule_note TEXT'))
            db.session.commit()

        # 历史数据回填：将incidents中的rule_note回填到alert_rules
        db.session.execute(text("""
            UPDATE alert_rules
            SET rule_note = (
                SELECT i.rule_note
                FROM incidents i
                WHERE i.rule_id = alert_rules.rule_id
                  AND i.rule_note IS NOT NULL
                  AND i.rule_note != ''
                ORDER BY i.created_at DESC
                LIMIT 1
            )
            WHERE (alert_rules.rule_note IS NULL OR alert_rules.rule_note = '')
        """))
        db.session.commit()

        # 兜底：老数据中该列可能为NULL，统一置为启用
        db.session.execute(text('UPDATE metric_configs SET is_active = 1 WHERE is_active IS NULL'))
        db.session.commit()


def _record_config_history(config_type, config_id, action, old_value=None, new_value=None, changed_by='system'):
    history = ConfigHistory(
        config_type=config_type,
        config_id=config_id,
        action=action,
        old_value=json.dumps(old_value, ensure_ascii=False) if old_value is not None else None,
        new_value=json.dumps(new_value, ensure_ascii=False) if new_value is not None else None,
        changed_by=changed_by
    )
    db.session.add(history)
    db.session.commit()


def ensure_default_metric_configs():
    ensure_schema_compatibility()
    changed = False
    for item in DEFAULT_METRIC_CONFIGS:
        exists = MetricConfig.query.filter_by(metric_key=item['metric_key']).first()
        if exists:
            if exists.is_active is None:
                exists.is_active = True
                changed = True
            if not exists.metric_name:
                exists.metric_name = item['metric_name']
                changed = True
            if not exists.description:
                exists.description = item['description']
                changed = True
            # 历史兼容：补齐未关闭指标的标准名称/描述
            if exists.metric_key == 'unclosed_count':
                if exists.metric_name in [None, '', '未关闭事件数']:
                    exists.metric_name = item['metric_name']
                    changed = True
                if not exists.description or '未关闭状态' not in exists.description:
                    exists.description = item['description']
                    changed = True
            if exists.metric_key == 'critical_count':
                if exists.metric_name in [None, '', 'Critical']:
                    exists.metric_name = item['metric_name']
                    changed = True
                if not exists.description or 'S1' not in exists.description:
                    exists.description = item['description']
                    changed = True
            if exists.metric_key == 'warning_count':
                if exists.metric_name in [None, '', 'Warning']:
                    exists.metric_name = item['metric_name']
                    changed = True
                if not exists.description or 'S2' not in exists.description:
                    exists.description = item['description']
                    changed = True
            if exists.metric_key == 'info_count':
                if exists.metric_name in [None, '', 'Info']:
                    exists.metric_name = item['metric_name']
                    changed = True
                if not exists.description or 'S3' not in exists.description:
                    exists.description = item['description']
                    changed = True
            if exists.metric_key == 'runbook':
                if not exists.description or 'rule_note' not in exists.description:
                    exists.description = item['description']
                    changed = True
            if exists.metric_key == 'quality_dimensions':
                if (not exists.description) or ('构成质量分数的评分维度，每个维度的权重分数综合组成质量得分' in exists.description):
                    exists.description = item['description']
                    changed = True
            continue

        db.session.add(
            MetricConfig(
                metric_key=item['metric_key'],
                metric_name=item['metric_name'],
                description=item['description'],
                is_active=True
            )
        )
        changed = True

    if changed:
        db.session.commit()


def ensure_default_time_windows():
    if TimeWindowConfig.query.count() > 0:
        return
    changed = False
    for item in DEFAULT_TIME_WINDOWS:
        exists = TimeWindowConfig.query.filter_by(window_name=item['window_name']).first()
        if exists:
            continue
        db.session.add(
            TimeWindowConfig(
                window_name=item['window_name'],
                window_type=item['window_type'],
                start_hour=item['start_hour'],
                end_hour=item['end_hour'],
                description=item['description'],
                is_active=True
            )
        )
        changed = True
    if changed:
        db.session.commit()


def ensure_default_invalid_rules():
    if InvalidAlertRule.query.count() > 0:
        return
    changed = False
    for index, item in enumerate(DEFAULT_INVALID_RULES):
        exists = (
            InvalidAlertRule.query
            .filter_by(rule_name=item['rule_name'], field_name=item['field_name'], operator=item['operator'], field_value=item['field_value'])
            .first()
        )
        if exists:
            continue
        db.session.add(
            InvalidAlertRule(
                rule_name=item['rule_name'],
                field_name=item['field_name'],
                operator=item['operator'],
                field_value=item['field_value'],
                description=item['description'],
                is_active=True,
                sort_order=index + 1
            )
        )
        changed = True
    if changed:
        db.session.commit()


def ensure_default_completion_conditions():
    if CompletionCondition.query.count() > 0:
        return
    for item in DEFAULT_COMPLETION_CONDITIONS:
        db.session.add(
            CompletionCondition(
                name=item['name'],
                type=item['type'],
                field=item['field'],
                value=item['value'],
                logic=item['logic'],
                guide=item['guide'],
                status=item['status'],
                sort_order=item['sort_order']
            )
        )
    db.session.commit()


def ensure_default_score_thresholds():
    changed = False
    for item in DEFAULT_SCORE_THRESHOLDS:
        threshold = ScoreThresholdConfig.query.filter_by(dimension_key=item['dimension_key']).first()
        if not threshold:
            db.session.add(ScoreThresholdConfig(**item, is_active=True))
            changed = True
            continue

        legacy = LEGACY_SCORE_THRESHOLD_DEFAULTS.get(item['dimension_key']) or {}
        current_type = (threshold.threshold_type or '').strip().lower()
        current_value = str(threshold.threshold_value or '').strip()
        legacy_type = (legacy.get('threshold_type') or '').strip().lower()
        legacy_value = str(legacy.get('threshold_value') or '').strip()

        # 仅自动迁移“历史默认值”，避免覆盖用户自定义阈值
        if current_type == legacy_type and current_value == legacy_value:
            threshold.threshold_type = item['threshold_type']
            threshold.threshold_value = item['threshold_value']
            threshold.score_direction = item['score_direction']
            threshold.description = item['description']
            changed = True
        elif item['dimension_key'] == 'event_count':
            # 方案1迁移：若仍是历史“单规则=25*天数”默认值，则切换到“单规则=1*天数”
            compact_value = current_value.replace(' ', '').lower()
            if current_type in ['le', 'lt'] and compact_value in ['25*period_days', 'period_days*25', '25xperiod_days', 'period_daysx25']:
                threshold.threshold_type = item['threshold_type']
                threshold.threshold_value = item['threshold_value']
                threshold.score_direction = item['score_direction']
                threshold.description = item['description']
                changed = True
        elif not threshold.description:
            threshold.description = item['description']
            changed = True

    # 事件数阈值拆分迁移：
    # 历史版本只有一个event_count阈值，很多场景会配置为 N*period_days（实为聚合口径）。
    # 迁移策略：当检测到N>1时，将N迁移到“多规则/天阈值”，并将单规则阈值恢复为1*period_days。
    try:
        ensure_default_api_configs()
        event_threshold = ScoreThresholdConfig.query.filter_by(dimension_key='event_count').first()
        aggregate_cfg = ApiConfig.query.filter_by(config_key='event_count_aggregate_threshold_per_day').first()
        if event_threshold and aggregate_cfg:
            current_type = (event_threshold.threshold_type or '').strip().lower()
            current_value = str(event_threshold.threshold_value or '').strip()
            compact = current_value.replace(' ', '').lower()

            def _extract_period_multiplier(compact_text):
                try:
                    if compact_text.endswith('*period_days'):
                        return float(compact_text[:-len('*period_days')])
                    if compact_text.startswith('period_days*'):
                        return float(compact_text[len('period_days*'):])
                    if compact_text.endswith('xperiod_days'):
                        return float(compact_text[:-len('xperiod_days')])
                    if compact_text.startswith('period_daysx'):
                        return float(compact_text[len('period_daysx'):])
                except Exception:
                    return None
                return None

            multiplier = _extract_period_multiplier(compact)
            if current_type in ['le', 'lt'] and multiplier and multiplier > 1:
                current_aggregate = _safe_int(aggregate_cfg.config_value, 25)
                if current_aggregate <= 0 or current_aggregate == 25:
                    aggregate_cfg.config_value = str(int(multiplier) if float(multiplier).is_integer() else multiplier)
                    changed = True
                event_threshold.threshold_type = 'le'
                event_threshold.threshold_value = '1*period_days'
                event_threshold.description = '告警事件数小于等于 1 * 统计周期天数时达标（单规则口径）'
                changed = True
    except Exception:
        pass

    if changed:
        db.session.commit()


def get_api_config_map():
    ensure_default_api_configs()
    configs = {}
    for config in ApiConfig.query.order_by(ApiConfig.id.asc()).all():
        configs[config.config_key] = config.config_value
    return configs


def _normalize_sync_window(start_date=None, end_date=None, sync_days=7):
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=max(1, sync_days))

    if end_dt < start_dt:
        raise ValueError('结束时间不能早于开始时间')

    normalized_start = start_dt.strftime('%Y-%m-%d')
    normalized_end = end_dt.strftime('%Y-%m-%d')
    start_ts = int(datetime.strptime(normalized_start, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(normalized_end, '%Y-%m-%d').timestamp()) + 86399
    return normalized_start, normalized_end, start_ts, end_ts


def _update_sync_history(history_id, **fields):
    history = SyncHistory.query.get(history_id)
    if not history:
        return
    for key, value in fields.items():
        setattr(history, key, value)
    db.session.commit()


def _fetch_remote_incidents(api_configs, start_ts, end_ts, history_id=None):
    api_url = api_configs.get('api_url') or DEFAULT_API_URL
    team_ids = _safe_json_list(api_configs.get('team_ids'), DEFAULT_TEAM_IDS)
    channel_ids = _safe_json_list(api_configs.get('channel_ids'), DEFAULT_CHANNEL_IDS)
    limit = min(100, max(1, _safe_int(api_configs.get('limit'), 100)))

    page = 1
    all_items = []
    fetch_pages = 0
    has_next_page = True
    while has_next_page:
        body = {
            'start_time': start_ts,
            'end_time': end_ts,
            'team_ids': team_ids,
            'limit': limit,
            'p': page,
            'channel_ids': channel_ids
        }
        response = requests.post(
            api_url,
            params={'app_key': DEFAULT_APP_KEY},
            json=body,
            timeout=60
        )
        if response.status_code != 200:
            raise RuntimeError(f'请求失败，HTTP状态码={response.status_code}')

        payload = response.json()
        error_obj = payload.get('error') if isinstance(payload, dict) else None
        if error_obj:
            raise RuntimeError(error_obj.get('message') or error_obj.get('code') or '远端接口返回错误')

        data_obj = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(data_obj, dict):
            raise RuntimeError('接口返回格式错误：缺少data对象')

        items = data_obj.get('items') or []
        if not isinstance(items, list):
            raise RuntimeError('接口返回格式错误：data.items不是数组')

        all_items.extend(items)
        fetch_pages += 1
        has_next_page = bool(data_obj.get('has_next_page'))
        page += 1

        if history_id:
            progress = min(55, 5 + fetch_pages * 5)
            _update_sync_history(
                history_id,
                progress=progress,
                total_items=len(all_items),
                message=f'已拉取{len(all_items)}条，分页{fetch_pages}'
            )

        # 避免p*limit超过10000导致接口报错，超过后提前停止
        if (page - 1) * limit >= 10000:
            has_next_page = False

    return all_items


def _build_alert_rule_records(incidents):
    rule_map = {}
    for incident in incidents:
        if not incident.rule_id:
            continue
        if incident.rule_id not in rule_map:
            rule_map[incident.rule_id] = {
                'rule_id': incident.rule_id,
                'rule_name': incident.rule_name,
                'runbook_url': incident.runbook_url,
                'rule_note': incident.rule_note,
                'scene_set': set()
            }

        current = rule_map[incident.rule_id]
        if incident.rule_name:
            current['rule_name'] = incident.rule_name
        if incident.runbook_url:
            current['runbook_url'] = incident.runbook_url
        if incident.rule_note:
            current['rule_note'] = incident.rule_note
        if incident.scene:
            current['scene_set'].add(str(incident.scene))

    rules = []
    for data in rule_map.values():
        rules.append(
            AlertRule(
                rule_id=data['rule_id'],
                rule_name=data.get('rule_name'),
                runbook_url=data.get('runbook_url'),
                rule_note=data.get('rule_note'),
                scene='|'.join(sorted(data['scene_set'])) if data['scene_set'] else None
            )
        )
    return rules


def run_sync_job(trigger_type='manual', start_date=None, end_date=None, task=None):
    if not SYNC_LOCK.acquire(blocking=False):
        raise RuntimeError('当前已有同步任务在执行，请稍后再试')

    history = None
    try:
        api_configs = get_api_config_map()
        sync_days = _safe_int(api_configs.get('sync_days'), 7)
        if task:
            sync_days = max(1, _safe_int(task.sync_days, sync_days))

        win_start, win_end, start_ts, end_ts = _normalize_sync_window(start_date, end_date, sync_days)
        history = SyncHistory(
            task_id=task.id if task else None,
            trigger_type=trigger_type,
            status='running',
            request_start_date=win_start,
            request_end_date=win_end,
            progress=1,
            message='开始同步'
        )
        db.session.add(history)
        db.session.commit()

        remote_items = _fetch_remote_incidents(api_configs, start_ts, end_ts, history.id)
        _update_sync_history(history.id, progress=60, total_items=len(remote_items), message='开始转换数据')

        converted_incidents = []
        for idx, item in enumerate(remote_items, start=1):
            incident = Incident.from_api_data(item)
            converted_incidents.append(incident)
            if idx % 100 == 0:
                progress = min(85, 60 + int((idx / max(1, len(remote_items))) * 25))
                _update_sync_history(history.id, progress=progress, message=f'数据转换中 {idx}/{len(remote_items)}')

        # 去重：同一批次若存在重复incident_id，保留最后一条
        dedup_map = {}
        for incident in converted_incidents:
            rid = str(getattr(incident, 'incident_id', '') or '').strip()
            if not rid:
                continue
            dedup_map[rid] = incident
        dedup_incidents = list(dedup_map.values())

        _update_sync_history(history.id, progress=88, message='开始按时间窗口覆盖写入本地数据')

        # 原子更新：
        # 1) 删除时间窗口内历史事件
        # 2) 删除与本次拉取incident_id重复的历史事件（兼容事件时间变更）
        # 3) 写入本次窗口数据
        # 4) 基于全量事件重建规则聚合表
        window_deleted_count = (
            Incident.query
            .filter(Incident.created_at >= start_ts, Incident.created_at <= end_ts)
            .delete(synchronize_session=False)
        )

        duplicate_deleted_count = 0
        if dedup_incidents:
            incoming_ids = [inc.incident_id for inc in dedup_incidents]
            duplicate_deleted_count = (
                Incident.query
                .filter(Incident.incident_id.in_(incoming_ids))
                .delete(synchronize_session=False)
            )
            db.session.bulk_save_objects(dedup_incidents)
            db.session.flush()

        all_incidents = Incident.query.all()
        rule_records = _build_alert_rule_records(all_incidents)
        AlertRule.query.delete(synchronize_session=False)
        if rule_records:
            db.session.bulk_save_objects(rule_records)
        db.session.commit()

        _update_sync_history(
            history.id,
            status='success',
            progress=100,
            total_items=len(remote_items),
            success_items=len(dedup_incidents),
            failed_items=0,
            message=f'同步完成，窗口覆盖更新{len(dedup_incidents)}条（删除窗口历史{window_deleted_count}条）',
            finished_at=datetime.now()
        )

        if task:
            task.last_run_at = datetime.now()
            task.next_run_at = _calc_next_run_at(task, datetime.now())
            db.session.commit()

        return {
            'history_id': history.id,
            'total_items': len(remote_items),
            'created': len(dedup_incidents),
            'deleted_in_window': window_deleted_count,
            'deleted_by_incident_id': duplicate_deleted_count,
            'start_date': win_start,
            'end_date': win_end
        }
    except Exception as e:
        db.session.rollback()
        if history:
            _update_sync_history(
                history.id,
                status='failed',
                progress=100,
                message=str(e),
                finished_at=datetime.now()
            )
        raise
    finally:
        SYNC_LOCK.release()


def _run_sync_job_in_background(trigger_type='manual', start_date=None, end_date=None, task_id=None):
    def _target():
        with app.app_context():
            try:
                task = SyncTask.query.get(task_id) if task_id else None
                run_sync_job(
                    trigger_type=trigger_type,
                    start_date=start_date,
                    end_date=end_date,
                    task=task
                )
            except Exception:
                app.logger.exception('后台同步任务执行失败')

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    return thread


def _run_due_sync_tasks():
    now = datetime.now()
    tasks = SyncTask.query.filter_by(is_active=True).all()
    for task in tasks:
        if task.next_run_at is None:
            task.next_run_at = _calc_next_run_at(task, now)
            db.session.commit()

        if task.next_run_at and task.next_run_at <= now:
            try:
                run_sync_job(trigger_type='auto', task=task)
            except Exception as err:
                task.last_run_at = now
                task.next_run_at = _calc_next_run_at(task, now)
                db.session.commit()
                failed_history = SyncHistory(
                    task_id=task.id,
                    trigger_type='auto',
                    status='failed',
                    request_start_date=None,
                    request_end_date=None,
                    progress=100,
                    message=str(err),
                    started_at=now,
                    finished_at=datetime.now()
                )
                db.session.add(failed_history)
                db.session.commit()


def _scheduler_loop():
    while True:
        try:
            with app.app_context():
                _run_due_sync_tasks()
        except Exception as e:
            print(f'[scheduler] {e}')
        time.sleep(30)


def _start_scheduler_once():
    global SCHEDULER_STARTED
    if SCHEDULER_STARTED:
        return
    with SCHEDULER_LOCK:
        if SCHEDULER_STARTED:
            return
        thread = threading.Thread(target=_scheduler_loop, daemon=True)
        thread.start()
        SCHEDULER_STARTED = True


@app.before_request
def bootstrap_once():
    global BOOTSTRAPPED
    if BOOTSTRAPPED:
        return
    with BOOTSTRAP_LOCK:
        if BOOTSTRAPPED:
            return
        db.create_all()
        ensure_schema_compatibility()
        ensure_default_metric_configs()
        ensure_default_time_windows()
        ensure_default_invalid_rules()
        ensure_default_completion_conditions()
        ensure_default_score_thresholds()
        ensure_default_api_configs()
        ensure_default_sync_task()
        _start_scheduler_once()
        BOOTSTRAPPED = True

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/hello', methods=['GET'])
def hello():
    return jsonify({'message': 'Hello, World!'})


def _parse_date_range_to_timestamps(start_date_str=None, end_date_str=None, default_days=None):
    start_ts = None
    end_ts = None

    if start_date_str:
        try:
            start_ts = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
        except ValueError:
            start_ts = None

    if end_date_str:
        try:
            end_ts = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
        except ValueError:
            end_ts = None

    if start_ts is not None and end_ts is None:
        end_ts = int(time.time()) + 86400
    if end_ts is not None and start_ts is None:
        window_days = max(1, default_days or 7)
        start_ts = end_ts - window_days * 86400

    if start_ts is None and end_ts is None and default_days:
        end_ts = int(time.time()) + 86400
        start_ts = end_ts - max(1, default_days) * 86400

    return start_ts, end_ts


def _build_incident_query(domain=None, sub_domain=None, system=None, rule_id=None, start_ts=None, end_ts=None):
    query = Incident.query
    if domain:
        query = query.filter(Incident.system_domain == domain)
    if sub_domain:
        query = query.filter(Incident.sub_domain == sub_domain)
    if system:
        query = query.filter(Incident.business_system == system)
    if rule_id:
        query = query.filter(Incident.rule_id == rule_id)
    if start_ts is not None:
        query = query.filter(Incident.created_at >= start_ts)
    if end_ts is not None:
        query = query.filter(Incident.created_at < end_ts)
    return query


def _distinct_rule_ids(query):
    rows = (
        query.filter(Incident.rule_id.isnot(None))
        .with_entities(Incident.rule_id)
        .distinct()
        .all()
    )
    return [row[0] for row in rows if row and row[0]]


def _calculate_avg_quality_score(rule_ids, start_ts=None, end_ts=None):
    if not rule_ids:
        return 0.0

    period_days = 30
    if start_ts is not None and end_ts is not None:
        period_days = max(1, round((end_ts - start_ts) / 86400))

    scores = []
    for rid in rule_ids:
        if start_ts is not None and end_ts is not None:
            rule = AlertRule.calculate_quality_score(rid, period_days, start_ts, end_ts, save_to_db=False)
            if rule and rule.quality_score is not None:
                scores.append(float(rule.quality_score))
        else:
            existing_rule = AlertRule.query.filter_by(rule_id=rid).first()
            if existing_rule and existing_rule.quality_score is not None:
                scores.append(float(existing_rule.quality_score))
            else:
                rule = AlertRule.calculate_quality_score(rid, period_days, save_to_db=False)
                if rule and rule.quality_score is not None:
                    scores.append(float(rule.quality_score))

    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 1)


def _calculate_invalid_count_from_incidents(incidents):
    invalid_rules = InvalidAlertRule.query.filter_by(is_active=True).order_by(InvalidAlertRule.sort_order).all()
    invalid_count = 0
    for inc in incidents:
        is_invalid = True
        for invalid_rule in invalid_rules:
            field_value = getattr(inc, invalid_rule.field_name, None)
            if field_value is None:
                is_invalid = False
                break

            rule_value = invalid_rule.field_value
            if invalid_rule.field_name in ['seconds_to_close', 'acknowledgements', 'seconds_to_ack']:
                try:
                    rule_value = int(invalid_rule.field_value)
                    field_value = int(field_value)
                except (ValueError, TypeError):
                    is_invalid = False
                    break

            if invalid_rule.operator == 'eq':
                if str(field_value) != str(rule_value):
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'ne':
                if str(field_value) == str(rule_value):
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'lt':
                if field_value >= rule_value:
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'gt':
                if field_value <= rule_value:
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'le':
                if field_value > rule_value:
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'ge':
                if field_value < rule_value:
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'contains':
                if str(rule_value) not in str(field_value):
                    is_invalid = False
                    break
            elif invalid_rule.operator == 'in':
                if str(field_value) not in str(rule_value).split(','):
                    is_invalid = False
                    break

        if is_invalid:
            invalid_count += 1

    return invalid_count


def _calculate_rule_jitter_rate(incidents, window_seconds=1800):
    """
    统一抖动率口径：
    - 规则维度滑动时间窗口统计
    - 分子：同一规则在窗口内重复发生的事件数（按事件计数）
    - 分母：有规则ID的事件总数
    """
    buckets = {}
    total_rule_events = 0

    for inc in incidents:
        rid = str(getattr(inc, 'rule_id', '') or '').strip()
        if not rid:
            continue
        total_rule_events += 1

        raw_ts = getattr(inc, 'created_at', None)
        try:
            ts = int(raw_ts)
        except (TypeError, ValueError):
            continue
        if ts <= 0:
            continue
        buckets.setdefault(rid, []).append(ts)

    if total_rule_events == 0:
        return 0.0

    duplicate_events = 0
    for timestamps in buckets.values():
        if len(timestamps) < 2:
            continue
        timestamps.sort()
        for i in range(1, len(timestamps)):
            if timestamps[i] - timestamps[i - 1] <= window_seconds:
                duplicate_events += 1

    return round((duplicate_events / total_rule_events) * 100, 1)

@app.route('/api/incidents', methods=['POST'])
def create_incidents():
    try:
        data = request.get_json()
        
        if not data or 'data' not in data or 'items' not in data['data']:
            return jsonify({'error': 'Invalid data format'}), 400
        
        items = data['data']['items']
        created_count = 0
        updated_count = 0
        errors = []
        
        for item in items:
            try:
                incident = Incident.from_api_data(item)
                
                existing = Incident.query.filter_by(incident_id=incident.incident_id).first()
                
                if existing:
                    for key, value in incident.__dict__.items():
                        if not key.startswith('_') and key != 'id':
                            setattr(existing, key, value)
                    updated_count += 1
                else:
                    db.session.add(incident)
                    created_count += 1
                
                # 同步更新规则表
                AlertRule.update_from_incident(incident if not existing else existing)
                    
            except Exception as e:
                errors.append({'incident_id': item.get('incident_id'), 'error': str(e)})
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'created': created_count,
            'updated': updated_count,
            'errors': errors
        }), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/incidents', methods=['GET'])
def get_incidents():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        query = Incident.query
        
        if domain:
            query = query.filter_by(system_domain=domain)
        
        if sub_domain:
            query = query.filter_by(sub_domain=sub_domain)
        
        if system:
            query = query.filter_by(business_system=system)
        
        if rule_id:
            query = query.filter_by(rule_id=rule_id)
        
        if start_date:
            try:
                start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
                query = query.filter(Incident.created_at >= start_timestamp)
            except ValueError:
                pass
        
        if end_date:
            try:
                end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()) + 86400
                query = query.filter(Incident.created_at < end_timestamp)
            except ValueError:
                pass
        
        # 执行查询并分页
        pagination = query.order_by(Incident.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
        
        incidents = pagination.items
        total = pagination.total
        
        # 转换为字典列表
        result = []
        for incident in incidents:
            result.append(incident.to_dict())
        
        return jsonify({
            'success': True,
            'data': result,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': pagination.pages
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/incidents/clear', methods=['POST'])
def clear_incidents():
    """
    清空历史告警事件数据（同时清空规则聚合表）
    """
    try:
        running = SyncHistory.query.filter_by(status='running').first()
        if running:
            return jsonify({'error': '当前已有同步任务在执行，请稍后再试'}), 409

        incident_count = Incident.query.count()
        rule_count = AlertRule.query.count()

        Incident.query.delete(synchronize_session=False)
        AlertRule.query.delete(synchronize_session=False)
        db.session.commit()

        return jsonify({
            'success': True,
            'deleted_incidents': incident_count,
            'deleted_rules': rule_count,
            'message': f'已清空事件数据{incident_count}条，规则数据{rule_count}条'
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/incidents/top-alerts', methods=['GET'])
def get_top_alerts():
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)

        base_query = _build_incident_query(domain, sub_domain, system, rule_id, start_ts, end_ts)
        grouped = (
            base_query
            .filter(Incident.rule_id.isnot(None), func.trim(Incident.rule_id) != '')
            .with_entities(
                Incident.rule_id.label('rule_id'),
                func.count(Incident.id).label('count'),
                func.max(Incident.created_at).label('latest_created_at')
            )
            .group_by(Incident.rule_id)
            .order_by(func.count(Incident.id).desc())
            .limit(200)
            .all()
        )

        result = []
        for row in grouped:
            latest_incident = (
                base_query
                .filter(Incident.rule_id == row.rule_id)
                .order_by(Incident.created_at.desc())
                .first()
            )
            result.append({
                'rule_id': row.rule_id,
                'title': latest_incident.title if latest_incident else None,
                'system_domain': latest_incident.system_domain if latest_incident else None,
                'sub_domain': latest_incident.sub_domain if latest_incident else None,
                'business_system': latest_incident.business_system if latest_incident else None,
                'count': row.count
            })

        return jsonify({'success': True, 'top_alerts': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/incidents/new-metrics', methods=['GET'])
def get_new_metrics():
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)

        query = _build_incident_query(domain, sub_domain, system, rule_id, start_ts, end_ts)
        incidents = query.all()
        total = len(incidents)

        night_count = 0
        change_count = 0
        for inc in incidents:
            hour_tag = (inc.hours or '').strip().lower()
            if hour_tag in ['sleep', 'off', 'night', 'nighttime', '夜间']:
                night_count += 1
            if hour_tag in ['work', 'change', 'change_window', '变更', '变更期间']:
                change_count += 1

        invalid_count = _calculate_invalid_count_from_incidents(incidents)
        escalation_count = sum(1 for inc in incidents if inc.escalations and inc.escalations > 0)

        invalid_rate = round((invalid_count / total) * 100, 1) if total else 0
        escalation_rate = round((escalation_count / total) * 100, 1) if total else 0
        jitter_rate = _calculate_rule_jitter_rate(incidents, window_seconds=1800)

        return jsonify({
            'success': True,
            'night_count': night_count,
            'change_count': change_count,
            'invalid_rate': invalid_rate,
            'escalation_rate': escalation_rate,
            'jitter_rate': jitter_rate
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/incidents/rule-coverage', methods=['GET'])
def get_rule_coverage():
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)

        incident_query = _build_incident_query(domain, sub_domain, system, rule_id, start_ts, end_ts)
        rule_ids = _distinct_rule_ids(incident_query)

        if not rule_ids:
            return jsonify({
                'success': True,
                'runbook_coverage': 0,
                'scene_coverage': 0,
                'rule_count': 0
            }), 200

        rule_query = AlertRule.query.filter(AlertRule.rule_id.in_(rule_ids))
        rule_count = rule_query.count()
        if rule_count == 0:
            return jsonify({
                'success': True,
                'runbook_coverage': 0,
                'scene_coverage': 0,
                'rule_count': 0
            }), 200

        runbook_count = rule_query.filter(
            AlertRule.rule_note.isnot(None),
            func.trim(AlertRule.rule_note) != ''
        ).count()
        scene_count = rule_query.filter(
            AlertRule.scene.isnot(None),
            func.trim(AlertRule.scene) != ''
        ).count()

        return jsonify({
            'success': True,
            'runbook_coverage': round((runbook_count / rule_count) * 100, 1),
            'scene_coverage': round((scene_count / rule_count) * 100, 1),
            'rule_count': rule_count
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/trend', methods=['GET'])
def get_metrics_trend():
    try:
        metric_key = request.args.get('metric_key', '').strip()
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        period = (request.args.get('period') or 'day').strip().lower()

        allowed_metrics = {
            'total_count',
            'critical_count',
            'warning_count',
            'info_count',
            'unclosed_count',
            'duplicate_rate',
            'mtta',
            'mttr'
        }
        if metric_key not in allowed_metrics:
            return jsonify({'error': f'不支持的指标: {metric_key}'}), 400

        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)
        if start_ts is None or end_ts is None:
            now = int(time.time()) + 86400
            end_ts = now
            start_ts = now - 7 * 86400

        step = 7 * 86400 if period == 'week' else 86400
        trend = []
        cursor = start_ts
        while cursor < end_ts:
            window_end = min(cursor + step, end_ts)
            incidents = _build_incident_query(domain, sub_domain, system, rule_id, cursor, window_end).all()
            value = _compute_metric_value_from_incidents(metric_key, incidents)
            if period == 'week':
                label = (
                    datetime.fromtimestamp(cursor).strftime('%m-%d')
                    + '~'
                    + datetime.fromtimestamp(window_end - 1).strftime('%m-%d')
                )
            else:
                label = datetime.fromtimestamp(cursor).strftime('%m-%d')
            trend.append({'date': label, 'value': value})
            cursor = window_end

        return jsonify({'success': True, 'metric_key': metric_key, 'trend': trend}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/detail', methods=['GET'])
def get_metrics_detail():
    try:
        metric_key = request.args.get('metric_key', '').strip()
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        sort_order = (request.args.get('sort_order') or 'desc').strip().lower()

        allowed_metrics = {
            'total_count',
            'critical_count',
            'warning_count',
            'info_count',
            'unclosed_count',
            'duplicate_rate',
            'mtta',
            'mttr'
        }
        if metric_key not in allowed_metrics:
            return jsonify({'error': f'不支持的指标: {metric_key}'}), 400

        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)
        incidents = _build_incident_query(domain, sub_domain, system, rule_id, start_ts, end_ts).all()

        details = []
        if metric_key == 'duplicate_rate':
            title_counter = {}
            for inc in incidents:
                normalized_title = str(inc.title or '').strip()
                if not normalized_title:
                    continue
                title_counter[normalized_title] = title_counter.get(normalized_title, 0) + 1

            for inc in incidents:
                normalized_title = str(inc.title or '').strip()
                if not normalized_title:
                    continue
                duplicate_count = title_counter.get(normalized_title, 0)
                if duplicate_count < 2:
                    continue
                details.append({
                    'incident_id': inc.incident_id,
                    'title': inc.title,
                    'rule_id': inc.rule_id,
                    'rule_name': inc.rule_name,
                    'severity': _normalize_severity_label(inc.severity),
                    'system_domain': inc.system_domain,
                    'sub_domain': inc.sub_domain,
                    'business_system': inc.business_system,
                    'progress': inc.progress,
                    'created_at': inc.created_at,
                    'created_at_datetime': datetime.fromtimestamp(inc.created_at).strftime('%Y-%m-%d %H:%M:%S') if inc.created_at else None,
                    'metric_value': duplicate_count
                })
        else:
            for inc in incidents:
                if not _incident_matches_metric(metric_key, inc):
                    continue
                details.append({
                    'incident_id': inc.incident_id,
                    'title': inc.title,
                    'rule_id': inc.rule_id,
                    'rule_name': inc.rule_name,
                    'severity': _normalize_severity_label(inc.severity),
                    'system_domain': inc.system_domain,
                    'sub_domain': inc.sub_domain,
                    'business_system': inc.business_system,
                    'progress': inc.progress,
                    'created_at': inc.created_at,
                    'created_at_datetime': datetime.fromtimestamp(inc.created_at).strftime('%Y-%m-%d %H:%M:%S') if inc.created_at else None,
                    'metric_value': _incident_metric_value(metric_key, inc)
                })

        reverse = sort_order != 'asc'
        details.sort(
            key=lambda x: (x.get('metric_value', 0), x.get('created_at') or 0),
            reverse=reverse
        )

        return jsonify({
            'success': True,
            'metric_key': metric_key,
            'details': details
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/incidents/<string:incident_id>', methods=['PUT'])
def update_incident(incident_id):
    try:
        incident = Incident.query.filter_by(incident_id=incident_id).first()
        
        if not incident:
            return jsonify({'error': 'Incident not found'}), 404
        
        data = request.get_json()
        
        if 'scene' in data:
            incident.scene = data['scene']
            # 同时更新规则表
            AlertRule.update_from_incident(incident)
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'data': incident.to_dict()
        }), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/incidents/<string:incident_id>', methods=['DELETE'])
def delete_incident(incident_id):
    try:
        incident = Incident.query.filter_by(incident_id=incident_id).first()
        
        if not incident:
            return jsonify({'error': 'Incident not found'}), 404
        
        db.session.delete(incident)
        db.session.commit()
        
        return jsonify({'success': True}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics/config', methods=['GET'])
def get_metrics_config():
    try:
        ensure_default_metric_configs()
        configs = MetricConfig.query.all()
        return jsonify({'configs': [c.to_dict() for c in configs]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics/config/<string:metric_key>', methods=['PUT'])
def update_metrics_config(metric_key):
    try:
        config = MetricConfig.query.filter_by(metric_key=metric_key).first()
        created = False
        
        if not config:
            # 如果不存在，则创建新配置
            config = MetricConfig(metric_key=metric_key)
            db.session.add(config)
            created = True
            old_snapshot = None
        else:
            old_snapshot = config.to_dict()
        
        data = request.get_json()
        
        if 'metric_name' in data:
            config.metric_name = data['metric_name']
            
        if 'description' in data:
            config.description = data['description']
            
        db.session.commit()

        _record_config_history(
            'metric',
            config.id,
            'create' if created else 'update',
            old_snapshot,
            config.to_dict(),
            data.get('changed_by') or 'system'
        )
        
        return jsonify({'success': True, 'config': config.to_dict()}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/init-defaults', methods=['POST'])
def init_default_metrics():
    try:
        before_count = MetricConfig.query.count()
        ensure_default_metric_configs()
        after_count = MetricConfig.query.count()
        return jsonify({
            'success': True,
            'message': f'指标配置初始化完成，新增 {max(0, after_count - before_count)} 条'
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-windows', methods=['GET'])
def get_time_windows():
    try:
        ensure_default_time_windows()
        windows = TimeWindowConfig.query.order_by(TimeWindowConfig.id.asc()).all()
        return jsonify({'windows': [w.to_dict() for w in windows]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-windows', methods=['POST'])
def create_time_window():
    try:
        data = request.get_json() or {}
        window = TimeWindowConfig(
            window_name=data.get('window_name'),
            window_type=data.get('window_type'),
            start_hour=_safe_int(data.get('start_hour'), 0),
            end_hour=_safe_int(data.get('end_hour'), 0),
            description=data.get('description'),
            is_active=bool(data.get('is_active', True))
        )
        db.session.add(window)
        db.session.commit()
        _record_config_history('time_window', window.id, 'create', None, window.to_dict(), data.get('changed_by') or 'system')
        return jsonify({'success': True, 'window': window.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-windows/<int:window_id>', methods=['PUT'])
def update_time_window(window_id):
    try:
        window = TimeWindowConfig.query.get(window_id)
        if not window:
            return jsonify({'error': 'Window not found'}), 404

        data = request.get_json() or {}
        old_value = window.to_dict()

        if 'window_name' in data:
            window.window_name = data.get('window_name')
        if 'window_type' in data:
            window.window_type = data.get('window_type')
        if 'start_hour' in data:
            window.start_hour = _safe_int(data.get('start_hour'), window.start_hour)
        if 'end_hour' in data:
            window.end_hour = _safe_int(data.get('end_hour'), window.end_hour)
        if 'description' in data:
            window.description = data.get('description')
        if 'is_active' in data:
            window.is_active = bool(data.get('is_active'))

        db.session.commit()
        _record_config_history('time_window', window.id, 'update', old_value, window.to_dict(), data.get('changed_by') or 'system')
        return jsonify({'success': True, 'window': window.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-windows/init-defaults', methods=['POST'])
def init_default_time_windows():
    try:
        before = TimeWindowConfig.query.count()
        ensure_default_time_windows()
        after = TimeWindowConfig.query.count()
        return jsonify({'success': True, 'created': max(0, after - before)}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/invalid-alert-rules', methods=['GET'])
def get_invalid_alert_rules():
    try:
        ensure_default_invalid_rules()
        rules = InvalidAlertRule.query.order_by(InvalidAlertRule.sort_order.asc(), InvalidAlertRule.id.asc()).all()
        return jsonify({'rules': [r.to_dict() for r in rules]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/invalid-alert-rules', methods=['POST'])
def create_invalid_alert_rule():
    try:
        data = request.get_json() or {}
        max_sort = db.session.query(func.max(InvalidAlertRule.sort_order)).scalar() or 0
        rule = InvalidAlertRule(
            rule_name=data.get('rule_name'),
            field_name=data.get('field_name'),
            operator=data.get('operator'),
            field_value=str(data.get('field_value') or ''),
            is_active=bool(data.get('is_active', True)),
            sort_order=_safe_int(data.get('sort_order'), max_sort + 1),
            description=data.get('description')
        )
        db.session.add(rule)
        db.session.commit()
        _record_config_history('invalid_alert', rule.id, 'create', None, rule.to_dict(), data.get('changed_by') or 'system')
        return jsonify({'success': True, 'rule': rule.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/invalid-alert-rules/<int:rule_id>', methods=['PUT'])
def update_invalid_alert_rule(rule_id):
    try:
        rule = InvalidAlertRule.query.get(rule_id)
        if not rule:
            return jsonify({'error': 'Rule not found'}), 404

        data = request.get_json() or {}
        old_value = rule.to_dict()
        if 'rule_name' in data:
            rule.rule_name = data.get('rule_name')
        if 'field_name' in data:
            rule.field_name = data.get('field_name')
        if 'operator' in data:
            rule.operator = data.get('operator')
        if 'field_value' in data:
            rule.field_value = str(data.get('field_value') or '')
        if 'is_active' in data:
            rule.is_active = bool(data.get('is_active'))
        if 'description' in data:
            rule.description = data.get('description')
        if 'sort_order' in data:
            rule.sort_order = _safe_int(data.get('sort_order'), rule.sort_order)

        db.session.commit()
        _record_config_history('invalid_alert', rule.id, 'update', old_value, rule.to_dict(), data.get('changed_by') or 'system')
        return jsonify({'success': True, 'rule': rule.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/invalid-alert-rules/init-defaults', methods=['POST'])
def init_default_invalid_rules():
    try:
        before = InvalidAlertRule.query.count()
        ensure_default_invalid_rules()
        after = InvalidAlertRule.query.count()
        return jsonify({'success': True, 'created': max(0, after - before)}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/config', methods=['GET'])
def get_dimensions_config():
    try:
        dimension_type = request.args.get('type')
        query = DimensionConfig.query
        if dimension_type:
            query = query.filter_by(dimension_type=dimension_type)
        configs = query.order_by(DimensionConfig.sort_order.asc(), DimensionConfig.id.asc()).all()
        return jsonify({'configs': [c.to_dict() for c in configs]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/config/<int:config_id>', methods=['PUT'])
def update_dimensions_config(config_id):
    try:
        config = DimensionConfig.query.get(config_id)
        
        if not config:
            return jsonify({'error': 'Config not found'}), 404

        old_snapshot = config.to_dict()
        
        data = request.get_json()
        
        if 'dimension_name' in data:
            config.dimension_name = data['dimension_name']
            
        db.session.commit()

        _record_config_history(
            'dimension',
            config.id,
            'update',
            old_snapshot,
            config.to_dict(),
            data.get('changed_by') or 'system'
        )
        
        return jsonify({'success': True, 'config': config.to_dict()}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/dimensions/init-from-incidents', methods=['POST'])
def init_dimensions_from_incidents():
    try:
        created = 0
        existing_keys = {item.dimension_key for item in DimensionConfig.query.all()}
        sort_order = db.session.query(func.max(DimensionConfig.sort_order)).scalar() or 0

        # 业务域
        domains = (
            db.session.query(Incident.system_domain)
            .filter(Incident.system_domain.isnot(None), Incident.system_domain != '')
            .distinct()
            .all()
        )
        for row in domains:
            domain = row[0]
            if domain in existing_keys:
                continue
            sort_order += 1
            db.session.add(DimensionConfig(
                dimension_type='domain',
                dimension_key=domain,
                dimension_name=domain,
                parent_key=None,
                sort_order=sort_order,
                is_active=True
            ))
            existing_keys.add(domain)
            created += 1

        # 业务子域
        sub_domains = (
            db.session.query(Incident.system_domain, Incident.sub_domain)
            .filter(Incident.sub_domain.isnot(None), Incident.sub_domain != '')
            .distinct()
            .all()
        )
        for row in sub_domains:
            parent_domain = row[0]
            sub_domain = row[1]
            if sub_domain in existing_keys:
                continue
            sort_order += 1
            db.session.add(DimensionConfig(
                dimension_type='sub_domain',
                dimension_key=sub_domain,
                dimension_name=sub_domain,
                parent_key=parent_domain,
                sort_order=sort_order,
                is_active=True
            ))
            existing_keys.add(sub_domain)
            created += 1

        # 业务系统
        systems = (
            db.session.query(Incident.sub_domain, Incident.business_system)
            .filter(Incident.business_system.isnot(None), Incident.business_system != '')
            .distinct()
            .all()
        )
        for row in systems:
            parent_sub_domain = row[0]
            system_name = row[1]
            if system_name in existing_keys:
                continue
            sort_order += 1
            db.session.add(DimensionConfig(
                dimension_type='system',
                dimension_key=system_name,
                dimension_name=system_name,
                parent_key=parent_sub_domain,
                sort_order=sort_order,
                is_active=True
            ))
            existing_keys.add(system_name)
            created += 1

        db.session.commit()
        return jsonify({'success': True, 'created': created}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/alert-rules', methods=['GET'])
def get_alert_rules():
    """
    获取所有告警规则及其质量得分
    """
    try:
        from datetime import datetime
        
        # 获取查询参数
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        sort_by = request.args.get('sort_by', 'quality_score')
        sort_order = request.args.get('sort_order', 'desc')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        search = (request.args.get('search') or '').strip()
        
        # 计算时间范围
        period_days = 30
        start_timestamp = None
        end_timestamp = None
        
        if start_date and end_date:
            try:
                start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
                end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()) + 86400
                period_days = round((end_timestamp - start_timestamp) / (24 * 3600))
                if period_days < 1:
                    period_days = 1
            except ValueError:
                pass
        
        # 构建查询
        query = AlertRule.query
        joined_incidents = False
        
        # 如果有域/子域/系统/时间筛选，需要关联Incident表
        if domain or sub_domain or system or start_date or end_date:
            from sqlalchemy.orm import joinedload
            query = query.join(Incident, AlertRule.rule_id == Incident.rule_id)
            joined_incidents = True
            
            if domain:
                query = query.filter(Incident.system_domain == domain)
            if sub_domain:
                query = query.filter(Incident.sub_domain == sub_domain)
            if system:
                query = query.filter(Incident.business_system == system)
            
            # 处理时间筛选
            if start_timestamp:
                query = query.filter(Incident.created_at >= start_timestamp)
            if end_timestamp:
                query = query.filter(Incident.created_at < end_timestamp)

        # 关键词搜索：支持规则名/规则ID/业务域/业务子域/业务系统
        if search:
            search_pattern = f"%{search}%"
            if not joined_incidents:
                query = query.outerjoin(Incident, AlertRule.rule_id == Incident.rule_id)
                joined_incidents = True
            query = query.filter(or_(
                AlertRule.rule_name.ilike(search_pattern),
                AlertRule.rule_id.ilike(search_pattern),
                Incident.system_domain.ilike(search_pattern),
                Incident.sub_domain.ilike(search_pattern),
                Incident.business_system.ilike(search_pattern)
            ))

        if joined_incidents:
            query = query.distinct(AlertRule.id)
        
        # 获取规则列表
        rules = query.all()

        # 统一口径：规则列表始终按当前筛选窗口实时重算质量分
        result = []
        for rule in rules:
            # 从关联的Incident中获取域信息
            sample_incident = Incident.query.filter_by(rule_id=rule.rule_id).first()
            system_domain = sample_incident.system_domain if sample_incident else None
            sub_domain = sample_incident.sub_domain if sample_incident else None
            business_system = sample_incident.business_system if sample_incident else None

            calculated_rule = AlertRule.calculate_quality_score(
                rule.rule_id,
                period_days,
                start_timestamp,
                end_timestamp,
                save_to_db=False
            )
            if not calculated_rule:
                continue

            result.append({
                'id': rule.id,
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'rule_link': rule.rule_link,
                'runbook_url': rule.runbook_url,
                'rule_note': rule.rule_note,
                'scene': rule.scene,
                'system_domain': system_domain,
                'sub_domain': sub_domain,
                'business_system': business_system,
                'quality_score': calculated_rule.quality_score,
                'event_count_score': calculated_rule.event_count_score,
                'runbook_score': calculated_rule.runbook_score,
                'scene_score': calculated_rule.scene_score,
                'invalid_rate_score': calculated_rule.invalid_rate_score,
                'escalation_rate_score': calculated_rule.escalation_rate_score,
                'mtta_rate_score': calculated_rule.mtta_rate_score,
                'mttr_rate_score': calculated_rule.mttr_rate_score,
                'jitter_rate_score': calculated_rule.jitter_rate_score,
                'raw_event_count': calculated_rule.raw_event_count,
                'raw_invalid_rate': calculated_rule.raw_invalid_rate,
                'raw_escalation_rate': calculated_rule.raw_escalation_rate,
                'raw_mtta_rate': calculated_rule.raw_mtta_rate,
                'raw_mttr_rate': calculated_rule.raw_mttr_rate,
                'raw_jitter_rate': calculated_rule.raw_jitter_rate,
                'has_runbook': bool(rule.rule_note and str(rule.rule_note).strip()),
                'has_scene': bool(rule.scene and str(rule.scene).strip()),
                'quality_score_updated_at': rule.quality_score_updated_at.isoformat() if rule.quality_score_updated_at else None
            })

        reverse = (sort_order or 'desc') == 'desc'
        if sort_by == 'rule_name':
            result.sort(key=lambda x: (x.get('rule_name') or ''), reverse=reverse)
        else:
            result.sort(key=lambda x: float(x.get('quality_score') or 0), reverse=reverse)
        
        return jsonify({
            'success': True,
            'rules': result
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alert-rules/quality-scores', methods=['GET'])
def get_alert_rule_quality_scores():
    # 兼容旧前端路径，复用现有规则质量得分接口
    return get_alert_rules()

@app.route('/api/alert-rules/<string:rule_id>/quality-score', methods=['GET'])
def get_alert_rule_quality_score(rule_id):
    """
    获取单个规则的告警质量得分
    """
    try:
        from datetime import datetime
        
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        period_days = 30
        start_timestamp = None
        end_timestamp = None
        
        if start_date_str and end_date_str:
            try:
                start_timestamp = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
                end_timestamp = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
                period_days = round((end_timestamp - start_timestamp) / (24 * 3600))
                if period_days < 1:
                    period_days = 1
            except ValueError:
                pass
        
        rule = AlertRule.query.filter_by(rule_id=rule_id).first()
        
        if not rule:
            return jsonify({'error': 'Rule not found'}), 404
        
        # 计算得分
        calculated_rule = AlertRule.calculate_quality_score(rule_id, period_days, start_timestamp, end_timestamp, save_to_db=False)
        
        if not calculated_rule:
            return jsonify({'error': 'Failed to calculate quality score'}), 500
        
        # 获取同口径事件集（与calculate_quality_score保持一致）
        query = Incident.query.filter_by(rule_id=rule_id)
        if start_timestamp is not None and end_timestamp is not None:
            query = query.filter(Incident.created_at >= start_timestamp, Incident.created_at < end_timestamp)
        else:
            default_end = int(datetime.now().timestamp())
            default_start = default_end - period_days * 24 * 3600
            query = query.filter(Incident.created_at >= default_start, Incident.created_at < default_end)

        incidents = query.order_by(Incident.created_at.desc()).all()
        sample_incident = incidents[0] if incidents else Incident.query.filter_by(rule_id=rule_id).order_by(Incident.created_at.desc()).first()
        
        raw_data = {
            'event_count': calculated_rule.raw_event_count if calculated_rule.raw_event_count is not None else len(incidents),
            'invalid_rate': calculated_rule.raw_invalid_rate or 0,
            'escalation_rate': calculated_rule.raw_escalation_rate or 0,
            'mtta_rate': calculated_rule.raw_mtta_rate or 0,
            'mttr_rate': calculated_rule.raw_mttr_rate or 0,
            'jitter_rate': calculated_rule.raw_jitter_rate or 0
        }
        
        return jsonify({
            'success': True,
            'rule_id': calculated_rule.rule_id,
            'rule_name': calculated_rule.rule_name,
            'quality_score': calculated_rule.quality_score,
            'runbook_url': calculated_rule.runbook_url,
            'rule_note': calculated_rule.rule_note,
            'scene': calculated_rule.scene,
            'senne': calculated_rule.scene,
            'system_domain': sample_incident.system_domain if sample_incident else None,
            'sub_domain': sample_incident.sub_domain if sample_incident else None,
            'business_system': sample_incident.business_system if sample_incident else None,
            'severity': sample_incident.severity if sample_incident else None,
            'raw_data': raw_data,
            'score_details': {
                'event_count_score': calculated_rule.event_count_score,
                'runbook_score': calculated_rule.runbook_score,
                'scene_score': calculated_rule.scene_score,
                'invalid_rate_score': calculated_rule.invalid_rate_score,
                'escalation_rate_score': calculated_rule.escalation_rate_score,
                'mtta_rate_score': calculated_rule.mtta_rate_score,
                'mttr_rate_score': calculated_rule.mttr_rate_score,
                'jitter_rate_score': calculated_rule.jitter_rate_score
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alert-rules/quality-score/<string:rule_id>', methods=['GET'])
def get_alert_rule_quality_score_compat(rule_id):
    # 兼容旧前端路径
    return get_alert_rule_quality_score(rule_id)

@app.route('/api/alert-rules/quality-score/<string:rule_id>/refresh', methods=['GET'])
def refresh_rule_quality_score(rule_id):
    """
    刷新单个规则的告警质量得分（重新计算）
    """
    try:
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        start_date = None
        end_date = None
        period_days = 30
        
        if start_date_str and end_date_str:
            try:
                from datetime import datetime
                start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
                end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
                period_days = round((end_date - start_date) / (24 * 3600))
                if period_days < 1:
                    period_days = 1
            except ValueError:
                pass
        
        rule = AlertRule.calculate_quality_score(rule_id, period_days, start_date, end_date)
        
        if not rule:
            return jsonify({'error': 'Rule not found'}), 404
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'rule_id': rule.rule_id,
            'rule_name': rule.rule_name,
            'quality_score': rule.quality_score
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alert-rules/quality-score/batch', methods=['POST'])
def batch_refresh_quality_scores():
    """
    批量刷新告警质量得分
    """
    try:
        data = request.get_json()
        rule_ids = data.get('rule_ids', [])
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        
        start_date = None
        end_date = None
        period_days = 30
        
        if start_date_str and end_date_str:
            try:
                from datetime import datetime
                start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
                end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
                period_days = round((end_date - start_date) / (24 * 3600))
                if period_days < 1:
                    period_days = 1
            except ValueError:
                pass
        
        results = []
        for rule_id in rule_ids:
            rule = AlertRule.calculate_quality_score(rule_id, period_days, start_date, end_date)
            if rule:
                results.append({
                    'rule_id': rule.rule_id,
                    'quality_score': rule.quality_score,
                    'status': 'success'
                })
            else:
                results.append({
                    'rule_id': rule_id,
                    'status': 'error',
                    'message': 'Rule not found'
                })
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'results': results
        }), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/alert-rules/quality-score/aggregate', methods=['GET'])
def get_quality_score_aggregate():
    """
    获取筛选范围内规则质量得分聚合值
    """
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        start_ts, end_ts = _parse_date_range_to_timestamps(start_date, end_date, default_days=7)

        base_query = _build_incident_query(domain, sub_domain, system, rule_id, start_ts, end_ts)
        rule_ids = _distinct_rule_ids(base_query)
        average_score = _calculate_avg_quality_score(rule_ids, start_ts, end_ts)

        score_change = None
        if start_ts is not None and end_ts is not None and end_ts > start_ts:
            window_seconds = end_ts - start_ts
            prev_start = start_ts - window_seconds
            prev_end = start_ts
            prev_query = _build_incident_query(domain, sub_domain, system, rule_id, prev_start, prev_end)
            prev_rule_ids = _distinct_rule_ids(prev_query)
            prev_avg = _calculate_avg_quality_score(prev_rule_ids, prev_start, prev_end)
            score_change = round(average_score - prev_avg, 1)

        return jsonify({
            'success': True,
            'average_score': average_score,
            'score_change': score_change,
            'rule_count': len(rule_ids)
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alert-rules/quality-score/trend', methods=['GET'])
def get_quality_score_trend():
    """
    获取告警质量得分趋势数据
    """
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        period = request.args.get('period', 'day')  # day 或 week

        start_date, end_date = _parse_date_range_to_timestamps(start_date_str, end_date_str, default_days=7)

        trend_dates = []
        trend_scores = []

        if period == 'week':
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + 7 * 86400, end_date)

                period_query = _build_incident_query(domain, sub_domain, system, rule_id, current_start, current_end)
                period_rule_ids = _distinct_rule_ids(period_query)
                score = _calculate_avg_quality_score(period_rule_ids, current_start, current_end)

                week_label = datetime.fromtimestamp(current_start).strftime('%m-%d') + '~' + datetime.fromtimestamp(current_end - 1).strftime('%m-%d')
                trend_dates.append(week_label)
                trend_scores.append(score)

                current_start = current_end
        else:
            current_date = start_date
            while current_date < end_date:
                day_start = current_date
                day_end = current_date + 86400

                period_query = _build_incident_query(domain, sub_domain, system, rule_id, day_start, day_end)
                period_rule_ids = _distinct_rule_ids(period_query)
                score = _calculate_avg_quality_score(period_rule_ids, day_start, day_end)

                trend_date = datetime.fromtimestamp(current_date).strftime('%m-%d')
                trend_dates.append(trend_date)
                trend_scores.append(score)

                current_date = day_end

        return jsonify({
            'success': True,
            'dates': trend_dates,
            'scores': trend_scores
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alert-rules/quality-score/by-dimension', methods=['GET'])
def get_quality_score_by_dimension():
    """
    获取按维度（子域/系统）统计的告警质量得分
    """
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        rule_id = request.args.get('rule_id')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        dimension = request.args.get('dimension', 'subdomain')  # subdomain 或 system

        start_date, end_date = _parse_date_range_to_timestamps(start_date_str, end_date_str, default_days=7)
        incident_query = _build_incident_query(domain, sub_domain, system, rule_id, start_date, end_date)
        incidents = incident_query.filter(Incident.rule_id.isnot(None)).all()

        dimension_rules = {}
        for inc in incidents:
            if dimension == 'subdomain':
                dim_name = f"{inc.system_domain or '未知'}/{inc.sub_domain or '未知'}"
            else:
                dim_name = inc.business_system or '未知'
            dimension_rules.setdefault(dim_name, set()).add(inc.rule_id)

        items = []
        for dim_name, rid_set in dimension_rules.items():
            rid_list = sorted([rid for rid in rid_set if rid])
            avg_score = _calculate_avg_quality_score(rid_list, start_date, end_date)
            items.append({
                'name': dim_name,
                'avg_score': avg_score,
                'rule_count': len(rid_list)
            })

        items.sort(key=lambda x: x['avg_score'], reverse=True)
        return jsonify({
            'success': True,
            'items': items
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/incidents/<string:rule_id>/incidents', methods=['GET'])
def get_rule_incidents(rule_id):
    """
    获取规则对应的所有告警事件明细
    """
    try:
        from datetime import datetime
        
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        query = Incident.query.filter_by(rule_id=rule_id)
        
        if start_date_str and end_date_str:
            try:
                start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
                end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
                query = query.filter(Incident.created_at >= start_date, Incident.created_at < end_date)
            except ValueError:
                pass
        
        incidents = query.order_by(Incident.created_at.desc()).all()
        
        # 获取规则的runbook_url、rule_note和scene
        rule = AlertRule.query.filter_by(rule_id=rule_id).first()
        runbook_url = rule.runbook_url if rule else None
        rule_note = rule.rule_note if rule else None
        scene = rule.scene if rule else None
        
        result = []
        for inc in incidents:
            effective_scene = inc.scene if (inc.scene and str(inc.scene).strip()) else scene
            effective_rule_note = inc.rule_note if (inc.rule_note and str(inc.rule_note).strip()) else rule_note
            effective_runbook_url = inc.runbook_url if (inc.runbook_url and str(inc.runbook_url).strip()) else runbook_url
            result.append({
                'id': inc.id,
                'title': inc.title,
                'rule_name': rule.rule_name if rule and rule.rule_name else inc.rule_name,
                'severity': inc.severity,
                'system_domain': inc.system_domain,
                'sub_domain': inc.sub_domain,
                'business_system': inc.business_system,
                'collaboration_space': inc.channel_name,
                'progress': inc.progress,
                'created_at': datetime.fromtimestamp(inc.created_at).strftime('%Y-%m-%d %H:%M:%S') if inc.created_at else None,
                'seconds_to_ack': inc.seconds_to_ack,
                'seconds_to_close': inc.seconds_to_close,
                'close_type': inc.closed_by,
                'escalation_count': inc.escalations,
                'scene': effective_scene,
                'rule_note': effective_rule_note,
                'runbook_url': effective_runbook_url
            })
        
        return jsonify({'incidents': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alert-rules/<string:rule_id>/incidents', methods=['GET'])
def get_rule_incidents_compat(rule_id):
    # 兼容旧前端路径
    return get_rule_incidents(rule_id)

@app.route('/api/metrics/<string:metric>/trend', methods=['GET'])
def get_metric_trend(metric):
    """
    获取指标趋势数据
    """
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        period = request.args.get('period', 'day')
        
        start_date = None
        end_date = None
        
        if start_date_str:
            try:
                start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
            except ValueError:
                pass
        if end_date_str:
            try:
                end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
            except ValueError:
                pass
        
        if not start_date or not end_date:
            end_date = int(time.time()) + 86400
            start_date = end_date - 7 * 86400
        
        trend_dates = []
        trend_values = []
        
        if period == 'week':
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + 7 * 86400, end_date)
                
                value = calculate_metric_value(metric, current_start, current_end, domain, sub_domain, system)
                
                week_label = datetime.fromtimestamp(current_start).strftime('%m-%d') + '~' + datetime.fromtimestamp(current_end - 1).strftime('%m-%d')
                trend_dates.append(week_label)
                trend_values.append(value)
                
                current_start = current_end
        else:
            current_date = start_date
            while current_date < end_date:
                day_start = current_date
                day_end = current_date + 86400
                
                value = calculate_metric_value(metric, day_start, day_end, domain, sub_domain, system)
                
                trend_date = datetime.fromtimestamp(current_date).strftime('%m-%d')
                trend_dates.append(trend_date)
                trend_values.append(value)
                
                current_date = day_end
        
        return jsonify({
            'success': True,
            'dates': trend_dates,
            'values': trend_values
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics/<string:metric>/by-dimension', methods=['GET'])
def get_metric_by_dimension(metric):
    """
    获取指标按维度统计数据
    """
    try:
        domain = request.args.get('domain')
        sub_domain = request.args.get('sub_domain')
        system = request.args.get('system')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        dimension = request.args.get('dimension', 'subdomain')
        
        start_date = None
        end_date = None
        period_days = 30
        
        if start_date_str and end_date_str:
            try:
                start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
                end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp()) + 86400
                period_days = round((end_date - start_date) / 86400)
                if period_days < 1:
                    period_days = 1
            except ValueError:
                pass
        
        rules = AlertRule.query.all()
        dimension_values = {}
        
        for rule in rules:
            if start_date and end_date:
                calculated_rule = AlertRule.calculate_quality_score(rule.rule_id, period_days, start_date, end_date, save_to_db=False)
            else:
                calculated_rule = rule
            
            sample_incident = Incident.query.filter_by(rule_id=rule.rule_id).first()
            if not sample_incident:
                continue
            
            if dimension == 'subdomain':
                dim_name = f"{sample_incident.system_domain or '未知'}/{sample_incident.sub_domain or '未知'}"
            else:
                dim_name = sample_incident.business_system or '未知'
            
            value = get_metric_value_from_rule(metric, calculated_rule)
            
            if dim_name not in dimension_values:
                dimension_values[dim_name] = {'values': [], 'rule_count': 0}
            
            dimension_values[dim_name]['values'].append(value)
            dimension_values[dim_name]['rule_count'] += 1
        
        items = []
        for dim_name, data in dimension_values.items():
            avg_value = round(sum(data['values']) / len(data['values']), 1) if data['values'] else 0
            items.append({
                'name': dim_name,
                'value': avg_value,
                'rule_count': data['rule_count']
            })
        
        return jsonify({
            'success': True,
            'items': items
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def calculate_metric_value(metric, start_time, end_time, domain=None, sub_domain=None, system=None):
    """
    计算指定时间范围内的指标值
    """
    query = Incident.query.filter(Incident.created_at >= start_time, Incident.created_at < end_time)
    
    if domain:
        query = query.filter_by(system_domain=domain)
    if sub_domain:
        query = query.filter_by(sub_domain=sub_domain)
    if system:
        query = query.filter_by(business_system=system)
    
    incidents = query.all()
    
    if not incidents:
        return 0
    
    if metric == 'invalid_rate':
        # 动态计算无效告警
        invalid_rules = InvalidAlertRule.query.filter_by(is_active=True).order_by(InvalidAlertRule.sort_order).all()
        invalid_count = 0
        
        for inc in incidents:
            is_invalid = True
            for invalid_rule in invalid_rules:
                field_value = getattr(inc, invalid_rule.field_name, None)
                if field_value is None:
                    is_invalid = False
                    break
                
                rule_value = invalid_rule.field_value
                if invalid_rule.field_name in ['seconds_to_close', 'acknowledgements', 'seconds_to_ack']:
                    try:
                        rule_value = int(invalid_rule.field_value)
                        field_value = int(field_value) if field_value is not None else 0
                    except (ValueError, TypeError):
                        is_invalid = False
                        break
                
                if invalid_rule.operator == 'eq':
                    if str(field_value) != str(rule_value):
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'ne':
                    if str(field_value) == str(rule_value):
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'lt':
                    if field_value >= rule_value:
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'gt':
                    if field_value <= rule_value:
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'le':
                    if field_value > rule_value:
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'ge':
                    if field_value < rule_value:
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'contains':
                    if str(rule_value) not in str(field_value):
                        is_invalid = False
                        break
                elif invalid_rule.operator == 'in':
                    if str(field_value) not in str(rule_value).split(','):
                        is_invalid = False
                        break
            
            if is_invalid:
                invalid_count += 1
        
        return round(invalid_count / len(incidents) * 100, 1) if incidents else 0
    elif metric == 'runbook':
        rule_ids = list(set(inc.rule_id for inc in incidents if inc.rule_id))
        if not rule_ids:
            return 0
        rules_with_runbook = AlertRule.query.filter(
            AlertRule.rule_id.in_(rule_ids),
            AlertRule.rule_note.isnot(None),
            func.trim(AlertRule.rule_note) != ''
        ).count()
        return round(rules_with_runbook / len(rule_ids) * 100, 1) if rule_ids else 0
    elif metric == 'scene':
        rule_ids = list(set(inc.rule_id for inc in incidents if inc.rule_id))
        if not rule_ids:
            return 0
        rules_with_scene = AlertRule.query.filter(
            AlertRule.rule_id.in_(rule_ids),
            AlertRule.scene.isnot(None),
            func.trim(AlertRule.scene) != ''
        ).count()
        return round(rules_with_scene / len(rule_ids) * 100, 1) if rule_ids else 0
    elif metric == 'escalation_rate':
        escalation_count = sum(1 for inc in incidents if inc.escalations and inc.escalations > 0)
        return round(escalation_count / len(incidents) * 100, 1) if incidents else 0
    elif metric == 'mtta_rate':
        # 口径与规则评分保持一致：分母使用筛选条件下总事件数
        mtta_threshold = 300
        mtta_ok_count = sum(1 for inc in incidents if inc.seconds_to_ack and inc.seconds_to_ack > 0 and inc.seconds_to_ack <= mtta_threshold)
        return round(mtta_ok_count / len(incidents) * 100, 1) if incidents else 0
    elif metric == 'mttr_rate':
        # 口径与规则评分保持一致：分母使用筛选条件下总事件数
        mttr_threshold = 1800
        mttr_ok_count = sum(1 for inc in incidents if inc.seconds_to_close and inc.seconds_to_close > 0 and inc.seconds_to_close <= mttr_threshold)
        return round(mttr_ok_count / len(incidents) * 100, 1) if incidents else 0
    elif metric == 'jitter_rate':
        # 规则维度：30分钟窗口内重复事件 / 规则事件总数
        return _calculate_rule_jitter_rate(incidents, window_seconds=1800)
    
    return 0

def get_metric_value_from_rule(metric, rule):
    """
    从规则对象获取指标值
    """
    if not rule:
        return 0
    
    if metric == 'invalid_rate':
        return rule.raw_invalid_rate or 0
    elif metric == 'runbook':
        return 100 if (rule.rule_note and str(rule.rule_note).strip()) else 0
    elif metric == 'scene':
        return 100 if (rule.scene and str(rule.scene).strip()) else 0
    elif metric == 'escalation_rate':
        return rule.raw_escalation_rate or 0
    elif metric == 'mtta_rate':
        return rule.raw_mtta_rate or 0
    elif metric == 'mttr_rate':
        return rule.raw_mttr_rate or 0
    elif metric == 'jitter_rate':
        return rule.raw_jitter_rate or 0
    
    return 0

@app.route('/api/config-history', methods=['GET'])
def get_config_history():
    """
    获取配置历史记录
    """
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        config_type = request.args.get('config_type')
        
        query = ConfigHistory.query
        
        if config_type:
            query = query.filter_by(config_type=config_type)
        
        pagination = query.order_by(ConfigHistory.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
        
        history_items = pagination.items
        total = pagination.total
        
        results = []
        for item in history_items:
            results.append(item.to_dict())
        
        return jsonify({
            'success': True,
            'data': results,
            'total': total,
            'page': page,
            'per_page': per_page
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/config-history/<int:history_id>/rollback', methods=['POST'])
def rollback_config(history_id):
    """
    回滚配置到指定历史版本
    """
    try:
        history = ConfigHistory.query.get(history_id)
        
        if not history:
            return jsonify({'error': 'History not found'}), 404
        
        # 根据配置类型回滚
        if history.config_type == 'metric':
            config = MetricConfig.query.get(history.config_id)
            if config:
                old_data = history.old_value
                for key, value in old_data.items():
                    if key in ['metric_name', 'description', 'is_active']:
                        setattr(config, key, value)
                db.session.commit()
        elif history.config_type == 'dimension':
            config = DimensionConfig.query.get(history.config_id)
            if config:
                old_data = history.old_value
                for key, value in old_data.items():
                    if key in ['dimension_name', 'is_active']:
                        setattr(config, key, value)
                db.session.commit()
        
        return jsonify({'success': True}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/completion-conditions', methods=['GET'])
def get_completion_conditions():
    """
    获取所有完成条件配置
    """
    try:
        ensure_default_completion_conditions()
        conditions = CompletionCondition.query.order_by(CompletionCondition.sort_order).all()
        return jsonify({
            'conditions': [c.to_dict() for c in conditions]
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/completion-conditions', methods=['POST'])
def create_completion_condition():
    """
    创建完成条件配置
    """
    try:
        data = request.get_json()
        
        condition = CompletionCondition(
            name=data.get('name'),
            type=data.get('type'),
            field=data.get('field'),
            value=data.get('value'),
            logic=data.get('logic', 'AND'),
            guide=data.get('guide', ''),
            status=data.get('status', 'enabled'),
            sort_order=data.get('sort_order', 0)
        )
        
        db.session.add(condition)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'condition': condition.to_dict()
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/completion-conditions/<int:condition_id>', methods=['PUT'])
def update_completion_condition(condition_id):
    """
    更新完成条件配置
    """
    try:
        condition = CompletionCondition.query.get(condition_id)
        if not condition:
            return jsonify({'error': 'Condition not found'}), 404
        
        data = request.get_json()
        
        if 'name' in data:
            condition.name = data['name']
        if 'type' in data:
            condition.type = data['type']
        if 'field' in data:
            condition.field = data['field']
        if 'value' in data:
            condition.value = data['value']
        if 'logic' in data:
            condition.logic = data['logic']
        if 'guide' in data:
            condition.guide = data['guide']
        if 'status' in data:
            condition.status = data['status']
        if 'sort_order' in data:
            condition.sort_order = data['sort_order']
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'condition': condition.to_dict()
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/completion-conditions/<int:condition_id>', methods=['DELETE'])
def delete_completion_condition(condition_id):
    """
    删除完成条件配置
    """
    try:
        condition = CompletionCondition.query.get(condition_id)
        if not condition:
            return jsonify({'error': 'Condition not found'}), 404
        
        db.session.delete(condition)
        db.session.commit()
        
        return jsonify({
            'success': True
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/score-thresholds', methods=['GET'])
def get_score_thresholds():
    """
    获取得分阈值配置
    """
    try:
        ensure_default_score_thresholds()
        ensure_default_api_configs()
        thresholds = ScoreThresholdConfig.query.all()
        aggregate_cfg = ApiConfig.query.filter_by(config_key='event_count_aggregate_threshold_per_day').first()
        aggregate_threshold_per_day = _safe_int(aggregate_cfg.config_value if aggregate_cfg else 25, 25)
        if aggregate_threshold_per_day < 1:
            aggregate_threshold_per_day = 25
        return jsonify({
            'thresholds': [t.to_dict() for t in thresholds],
            'event_count_aggregate_threshold_per_day': aggregate_threshold_per_day
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/score-thresholds/init', methods=['POST'])
def init_score_thresholds():
    try:
        created = 0
        for item in DEFAULT_SCORE_THRESHOLDS:
            exists = ScoreThresholdConfig.query.filter_by(dimension_key=item['dimension_key']).first()
            if exists:
                continue
            db.session.add(ScoreThresholdConfig(**item, is_active=True))
            created += 1
        db.session.commit()

        return jsonify({
            'success': True,
            'message': f'阈值配置初始化完成，新增 {created} 条'
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/score-thresholds/<int:threshold_id>', methods=['PUT'])
def update_score_threshold(threshold_id):
    """
    更新得分阈值配置
    """
    try:
        threshold = ScoreThresholdConfig.query.get(threshold_id)
        if not threshold:
            return jsonify({'error': 'Threshold not found'}), 404
        
        data = request.get_json()
        
        if 'weight' in data:
            threshold.weight = data['weight']
        if 'threshold_type' in data:
            threshold.threshold_type = data['threshold_type']
        if 'threshold_value' in data:
            threshold.threshold_value = data['threshold_value']
        if 'is_active' in data:
            threshold.is_active = data['is_active']
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'threshold': threshold.to_dict()
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/score-thresholds/batch', methods=['POST'])
def update_score_thresholds_batch():
    """
    批量更新得分阈值配置
    """
    try:
        data = request.get_json() or {}
        configs = data.get('configs') or []
        changed_by = data.get('changed_by') or 'system'
        aggregate_threshold_per_day = data.get('event_count_aggregate_threshold_per_day')

        if not isinstance(configs, list) or not configs:
            return jsonify({'error': 'configs不能为空且必须为数组'}), 400

        update_items = []
        total_weight = 0

        for item in configs:
            if not isinstance(item, dict):
                return jsonify({'error': 'configs中每项必须为对象'}), 400

            dimension_key = (item.get('dimension_key') or '').strip()
            if not dimension_key:
                return jsonify({'error': 'dimension_key不能为空'}), 400

            threshold = ScoreThresholdConfig.query.filter_by(dimension_key=dimension_key).first()
            if not threshold:
                return jsonify({'error': f'未找到维度配置: {dimension_key}'}), 404

            weight = _safe_int(item.get('weight'), None)
            if weight is None or weight < 0 or weight > 100:
                return jsonify({'error': f'{dimension_key} 的weight必须在0-100之间'}), 400

            threshold_type = (item.get('threshold_type') or threshold.threshold_type or '').strip().lower()
            if threshold_type not in ['lt', 'le', 'gt', 'ge', 'eq', 'ne', 'bool']:
                return jsonify({'error': f'{dimension_key} 的threshold_type无效'}), 400

            threshold_value = item.get('threshold_value', threshold.threshold_value)
            if threshold_value is None:
                threshold_value = ''
            threshold_value = str(threshold_value).strip()

            total_weight += weight
            update_items.append((threshold, {
                'weight': weight,
                'threshold_type': threshold_type,
                'threshold_value': threshold_value
            }))

        if total_weight != 100:
            return jsonify({'error': f'权重总和必须为100，当前为{total_weight}'}), 400

        updated = []
        for threshold, new_vals in update_items:
            old_snapshot = threshold.to_dict()
            threshold.weight = new_vals['weight']
            threshold.threshold_type = new_vals['threshold_type']
            threshold.threshold_value = new_vals['threshold_value']
            db.session.add(threshold)
            db.session.flush()
            history = ConfigHistory(
                config_type='score_threshold',
                config_id=threshold.id,
                action='update',
                old_value=json.dumps(old_snapshot, ensure_ascii=False),
                new_value=json.dumps(threshold.to_dict(), ensure_ascii=False),
                changed_by=changed_by
            )
            db.session.add(history)
            updated.append(threshold.to_dict())

        # 多规则阈值（每天）与权重配置一起保存，避免口径配置分散
        aggregate_updated_value = None
        if aggregate_threshold_per_day is not None:
            ensure_default_api_configs()
            aggregate_num = max(1, _safe_int(aggregate_threshold_per_day, 25))
            aggregate_cfg = ApiConfig.query.filter_by(config_key='event_count_aggregate_threshold_per_day').first()
            if aggregate_cfg:
                old_value = aggregate_cfg.config_value
                aggregate_cfg.config_value = str(aggregate_num)
                db.session.add(aggregate_cfg)
                db.session.flush()
                aggregate_updated_value = aggregate_cfg.config_value
                history = ConfigHistory(
                    config_type='api_config',
                    config_id=aggregate_cfg.id,
                    action='update',
                    old_value=json.dumps({'config_key': aggregate_cfg.config_key, 'config_value': old_value}, ensure_ascii=False),
                    new_value=json.dumps({'config_key': aggregate_cfg.config_key, 'config_value': aggregate_cfg.config_value}, ensure_ascii=False),
                    changed_by=changed_by
                )
                db.session.add(history)

        db.session.commit()

        return jsonify({
            'success': True,
            'updated_count': len(updated),
            'thresholds': updated,
            'event_count_aggregate_threshold_per_day': aggregate_updated_value
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/api-config', methods=['GET'])
def get_api_config():
    """
    获取API配置
    """
    try:
        ensure_default_api_configs()
        configs = ApiConfig.query.order_by(ApiConfig.id.asc()).all()
        return jsonify({'configs': [c.to_dict() for c in configs]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/api-config/<int:config_id>', methods=['PUT'])
def update_api_config(config_id):
    """
    更新API配置
    """
    try:
        config = ApiConfig.query.get(config_id)
        if not config:
            return jsonify({'error': 'Config not found'}), 404
        
        data = request.get_json() or {}

        if 'config_value' in data:
            new_value = str(data['config_value']).strip()
            if config.config_key == 'app_key':
                new_value = DEFAULT_APP_KEY
            elif config.config_key in ['team_ids', 'channel_ids']:
                parsed_list = _safe_json_list(new_value, None)
                if not isinstance(parsed_list, list):
                    return jsonify({'error': f'{config.config_key} 必须是JSON数组'}), 400
                new_value = json.dumps(parsed_list, ensure_ascii=False)
            elif config.config_key == 'sync_days':
                sync_days = max(1, _safe_int(new_value, 7))
                new_value = str(sync_days)
            elif config.config_key == 'limit':
                new_value = '100'
            elif config.config_key == 'event_count_aggregate_threshold_per_day':
                threshold_num = max(1, _safe_int(new_value, 25))
                new_value = str(threshold_num)
            elif config.config_key == 'api_url' and not new_value:
                return jsonify({'error': 'api_url不能为空'}), 400

            config.config_value = new_value
        if 'description' in data:
            config.description = data['description']
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'config': config.to_dict()
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/sync-tasks', methods=['GET'])
def get_sync_tasks():
    try:
        ensure_default_sync_task()
        tasks = SyncTask.query.order_by(SyncTask.id.asc()).all()
        return jsonify({'tasks': [t.to_dict() for t in tasks]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-tasks', methods=['POST'])
def create_sync_task():
    try:
        data = request.get_json() or {}
        task_name = (data.get('task_name') or '').strip()
        if not task_name:
            return jsonify({'error': 'task_name不能为空'}), 400

        frequency_type = (data.get('frequency_type') or 'daily').lower()
        if frequency_type not in ['daily', 'hourly']:
            return jsonify({'error': 'frequency_type仅支持daily/hourly'}), 400

        run_time = data.get('run_time') or '00:30'
        hourly_interval = max(1, _safe_int(data.get('hourly_interval'), 24))
        sync_days = max(1, _safe_int(data.get('sync_days'), 7))

        task = SyncTask(
            task_name=task_name,
            frequency_type=frequency_type,
            run_time=run_time,
            hourly_interval=hourly_interval,
            sync_days=sync_days,
            is_active=bool(data.get('is_active', True))
        )
        task.next_run_at = _calc_next_run_at(task, datetime.now())
        db.session.add(task)
        db.session.commit()

        return jsonify({'success': True, 'task': task.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-tasks/<int:task_id>', methods=['PUT'])
def update_sync_task(task_id):
    try:
        task = SyncTask.query.get(task_id)
        if not task:
            return jsonify({'error': 'Task not found'}), 404

        data = request.get_json() or {}
        if 'task_name' in data:
            task.task_name = (data.get('task_name') or '').strip() or task.task_name
        if 'frequency_type' in data:
            frequency_type = (data.get('frequency_type') or 'daily').lower()
            if frequency_type not in ['daily', 'hourly']:
                return jsonify({'error': 'frequency_type仅支持daily/hourly'}), 400
            task.frequency_type = frequency_type
        if 'run_time' in data:
            task.run_time = data.get('run_time') or '00:30'
        if 'hourly_interval' in data:
            task.hourly_interval = max(1, _safe_int(data.get('hourly_interval'), 24))
        if 'sync_days' in data:
            task.sync_days = max(1, _safe_int(data.get('sync_days'), 7))
        if 'is_active' in data:
            task.is_active = bool(data.get('is_active'))

        task.next_run_at = _calc_next_run_at(task, datetime.now())
        db.session.commit()
        return jsonify({'success': True, 'task': task.to_dict()}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-tasks/<int:task_id>', methods=['DELETE'])
def delete_sync_task(task_id):
    try:
        task = SyncTask.query.get(task_id)
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        db.session.delete(task)
        db.session.commit()
        return jsonify({'success': True}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-tasks/<int:task_id>/run', methods=['POST'])
def run_sync_task_now(task_id):
    try:
        task = SyncTask.query.get(task_id)
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        running = SyncHistory.query.filter_by(status='running').first()
        if running:
            return jsonify({'error': '当前已有同步任务在执行，请稍后再试'}), 409

        _run_sync_job_in_background(trigger_type='manual', task_id=task.id)
        return jsonify({
            'success': True,
            'started': True,
            'message': '同步任务已启动'
        }), 202
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 409
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-history', methods=['GET'])
def get_sync_history():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        trigger_type = request.args.get('trigger_type')

        query = SyncHistory.query
        if trigger_type in ['manual', 'auto']:
            query = query.filter_by(trigger_type=trigger_type)

        pagination = query.order_by(SyncHistory.id.desc()).paginate(page=page, per_page=per_page, error_out=False)
        return jsonify({
            'success': True,
            'history': [h.to_dict() for h in pagination.items],
            'total': pagination.total,
            'page': page,
            'per_page': per_page
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-progress/current', methods=['GET'])
def get_current_sync_progress():
    try:
        trigger_type = request.args.get('trigger_type')
        query = SyncHistory.query
        if trigger_type in ['manual', 'auto']:
            query = query.filter_by(trigger_type=trigger_type)

        running = query.filter_by(status='running').order_by(SyncHistory.id.desc()).first()
        latest = running or query.order_by(SyncHistory.id.desc()).first()
        return jsonify({
            'success': True,
            'running': bool(running),
            'progress': latest.to_dict() if latest else None
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-data/start', methods=['POST'])
def start_sync_data():
    """
    异步启动手动同步任务，立即返回，进度通过轮询接口查询
    """
    try:
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        running = SyncHistory.query.filter_by(status='running').first()
        if running:
            return jsonify({'error': '当前已有同步任务在执行，请稍后再试'}), 409

        api_configs = get_api_config_map()
        sync_days = _safe_int(api_configs.get('sync_days'), 7)
        _normalize_sync_window(start_date, end_date, sync_days)

        _run_sync_job_in_background(
            trigger_type='manual',
            start_date=start_date,
            end_date=end_date
        )
        return jsonify({
            'success': True,
            'started': True,
            'message': '同步任务已启动'
        }), 202
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync-data', methods=['POST'])
def sync_data():
    """
    手动同步数据
    """
    try:
        data = request.get_json() or {}
        result = run_sync_job(
            trigger_type='manual',
            start_date=data.get('start_date'),
            end_date=data.get('end_date')
        )
        return jsonify({'success': True, **result}), 200
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 409
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        ensure_schema_compatibility()
        ensure_default_metric_configs()
        ensure_default_time_windows()
        ensure_default_invalid_rules()
        ensure_default_completion_conditions()
        ensure_default_score_thresholds()
        ensure_default_api_configs()
        ensure_default_sync_task()
    app.run(debug=True, port=5000)
