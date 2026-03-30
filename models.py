from flask_sqlalchemy import SQLAlchemy
import json
from datetime import datetime

# 创建数据库对象
db = SQLAlchemy()


class MetricConfig(db.Model):
    __tablename__ = 'metric_configs'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    metric_key = db.Column(db.String(50), unique=True, nullable=False, comment='指标键名')
    metric_name = db.Column(db.String(100), nullable=False, comment='指标名称')
    description = db.Column(db.Text, nullable=True, comment='指标描述')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'metric_key': self.metric_key,
            'metric_name': self.metric_name,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class DimensionConfig(db.Model):
    __tablename__ = 'dimension_configs'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dimension_type = db.Column(db.String(50), nullable=False, comment='维度类型')
    dimension_key = db.Column(db.String(50), unique=True, nullable=False, comment='维度键名')
    dimension_name = db.Column(db.String(100), nullable=False, comment='维度名称')
    parent_key = db.Column(db.String(50), nullable=True, comment='父维度键名')
    description = db.Column(db.Text, nullable=True, comment='维度描述')
    sort_order = db.Column(db.Integer, default=0, comment='排序顺序')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'dimension_type': self.dimension_type,
            'dimension_key': self.dimension_key,
            'dimension_name': self.dimension_name,
            'parent_key': self.parent_key,
            'description': self.description,
            'sort_order': self.sort_order,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class InvalidAlertRule(db.Model):
    __tablename__ = 'invalid_alert_rules'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rule_name = db.Column(db.String(100), nullable=False, comment='规则名称')
    field_name = db.Column(db.String(50), nullable=False, comment='字段名')
    operator = db.Column(db.String(20), nullable=False, comment='操作符(eq/ne/lt/gt/le/ge/contains/in)')
    field_value = db.Column(db.String(500), nullable=False, comment='比较值')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    sort_order = db.Column(db.Integer, default=0, comment='排序顺序')
    description = db.Column(db.Text, nullable=True, comment='规则描述')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'rule_name': self.rule_name,
            'field_name': self.field_name,
            'operator': self.operator,
            'field_value': self.field_value,
            'is_active': self.is_active,
            'sort_order': self.sort_order,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class TimeWindowConfig(db.Model):
    __tablename__ = 'time_window_configs'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    window_name = db.Column(db.String(50), nullable=False, comment='窗口名称(night/change)')
    window_type = db.Column(db.String(50), nullable=False, comment='窗口类型')
    start_hour = db.Column(db.Integer, nullable=False, comment='开始小时(0-23)')
    end_hour = db.Column(db.Integer, nullable=False, comment='结束小时(0-23)')
    description = db.Column(db.Text, nullable=True, comment='窗口描述')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'window_name': self.window_name,
            'window_type': self.window_type,
            'start_hour': self.start_hour,
            'end_hour': self.end_hour,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class ConfigHistory(db.Model):
    __tablename__ = 'config_history'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    config_type = db.Column(db.String(50), nullable=False, comment='配置类型(metric/dimension/invalid_alert/time_window)')
    config_id = db.Column(db.Integer, nullable=False, comment='配置ID')
    action = db.Column(db.String(20), nullable=False, comment='操作类型(create/update/delete)')
    old_value = db.Column(db.Text, nullable=True, comment='修改前的值(JSON)')
    new_value = db.Column(db.Text, nullable=True, comment='修改后的值(JSON)')
    changed_by = db.Column(db.String(100), nullable=True, comment='修改人')
    change_reason = db.Column(db.Text, nullable=True, comment='修改原因')
    version = db.Column(db.Integer, default=1, comment='版本号')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'config_type': self.config_type,
            'config_id': self.config_id,
            'action': self.action,
            'old_value': json.loads(self.old_value) if self.old_value else None,
            'new_value': json.loads(self.new_value) if self.new_value else None,
            'changed_by': self.changed_by,
            'change_reason': self.change_reason,
            'version': self.version,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class ScoreThresholdConfig(db.Model):
    __tablename__ = 'score_threshold_configs'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dimension_key = db.Column(db.String(50), unique=True, nullable=False, comment='维度标识(event_count/runbook/scene/invalid_rate/escalation_rate/mtta_rate/mttr_rate/jitter_rate)')
    dimension_name = db.Column(db.String(100), nullable=False, comment='维度名称')
    weight = db.Column(db.Integer, default=10, comment='权重(0-100)')
    threshold_type = db.Column(db.String(20), nullable=False, comment='阈值类型(lt/le/gt/ge/eq/bool)')
    threshold_value = db.Column(db.String(100), nullable=False, comment='阈值')
    score_direction = db.Column(db.String(10), default='positive', comment='得分方向(positive正向/negative负向)')
    description = db.Column(db.Text, nullable=True, comment='描述')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'dimension_key': self.dimension_key,
            'dimension_name': self.dimension_name,
            'weight': self.weight,
            'threshold_type': self.threshold_type,
            'threshold_value': self.threshold_value,
            'score_direction': self.score_direction,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class ApiConfig(db.Model):
    __tablename__ = 'api_configs'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    config_key = db.Column(db.String(50), unique=True, nullable=False, comment='配置键(api_url/app_key/team_ids/channel_ids/sync_frequency/sync_days)')
    config_value = db.Column(db.Text, nullable=False, comment='配置值')
    description = db.Column(db.Text, nullable=True, comment='配置描述')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'config_key': self.config_key,
            'config_value': self.config_value,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class SyncTask(db.Model):
    __tablename__ = 'sync_tasks'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    task_name = db.Column(db.String(100), nullable=False, comment='任务名称')
    frequency_type = db.Column(db.String(20), default='daily', comment='执行频率类型(daily/hourly)')
    run_time = db.Column(db.String(10), default='00:30', comment='执行时间(HH:MM)，daily时生效')
    hourly_interval = db.Column(db.Integer, default=24, comment='按小时执行间隔(hourly时生效)')
    sync_days = db.Column(db.Integer, default=7, comment='每次同步最近N天数据')
    is_active = db.Column(db.Boolean, default=True, comment='是否启用')
    last_run_at = db.Column(db.DateTime, nullable=True, comment='最近执行时间')
    next_run_at = db.Column(db.DateTime, nullable=True, comment='下次执行时间')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    def to_dict(self):
        return {
            'id': self.id,
            'task_name': self.task_name,
            'frequency_type': self.frequency_type,
            'run_time': self.run_time,
            'hourly_interval': self.hourly_interval,
            'sync_days': self.sync_days,
            'is_active': self.is_active,
            'last_run_at': self.last_run_at.isoformat() if self.last_run_at else None,
            'next_run_at': self.next_run_at.isoformat() if self.next_run_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class SyncHistory(db.Model):
    __tablename__ = 'sync_history'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    task_id = db.Column(db.Integer, nullable=True, comment='关联任务ID，手动同步为空')
    trigger_type = db.Column(db.String(20), nullable=False, comment='触发类型(manual/auto)')
    status = db.Column(db.String(20), default='running', comment='执行状态(running/success/failed)')
    request_start_date = db.Column(db.String(20), nullable=True, comment='请求开始日期')
    request_end_date = db.Column(db.String(20), nullable=True, comment='请求结束日期')
    total_items = db.Column(db.Integer, default=0, comment='总拉取条数')
    success_items = db.Column(db.Integer, default=0, comment='成功导入条数')
    failed_items = db.Column(db.Integer, default=0, comment='失败条数')
    progress = db.Column(db.Integer, default=0, comment='进度百分比')
    message = db.Column(db.Text, nullable=True, comment='执行消息')
    detail = db.Column(db.Text, nullable=True, comment='执行详情JSON')
    started_at = db.Column(db.DateTime, default=datetime.now, comment='开始时间')
    finished_at = db.Column(db.DateTime, nullable=True, comment='结束时间')

    def to_dict(self):
        return {
            'id': self.id,
            'task_id': self.task_id,
            'trigger_type': self.trigger_type,
            'status': self.status,
            'request_start_date': self.request_start_date,
            'request_end_date': self.request_end_date,
            'total_items': self.total_items,
            'success_items': self.success_items,
            'failed_items': self.failed_items,
            'progress': self.progress,
            'message': self.message,
            'detail': json.loads(self.detail) if self.detail else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None
        }


class Incident(db.Model):
    __tablename__ = 'incidents'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    
    incident_id = db.Column(db.String(100), unique=True, nullable=False, comment='故障ID')
    title = db.Column(db.String(500), nullable=False, comment='故障标题')
    rule_id = db.Column(db.String(100), nullable=True, comment='规则id')
    rule_name = db.Column(db.String(500), nullable=True, comment='规则名称')
    severity = db.Column(db.String(50), nullable=True, comment='严重程度')
    system_domain = db.Column(db.String(100), nullable=True, comment='业务域')
    sub_domain = db.Column(db.String(100), nullable=True, comment='业务子域')
    business_system = db.Column(db.String(100), nullable=True, comment='业务系统')
    progress = db.Column(db.String(50), nullable=True, comment='处理进度')
    channel_id = db.Column(db.String(100), nullable=True, comment='协作空间id')
    channel_name = db.Column(db.String(200), nullable=True, comment='协作空间')
    team_id = db.Column(db.String(100), nullable=True, comment='团队id')
    team_name = db.Column(db.String(200), nullable=True, comment='归属团队')
    from_source = db.Column(db.String(100), nullable=True, comment='告警来源')
    created_at = db.Column(db.Integer, nullable=True, comment='触发时间(时间戳)')
    created_at_datetime = db.Column(db.DateTime, nullable=True, comment='触发时间(日期时间)')
    seconds_to_ack = db.Column(db.Integer, nullable=True, comment='认领耗时(秒)')
    seconds_to_close = db.Column(db.Integer, nullable=True, comment='关闭耗时(秒)')
    closed_by = db.Column(db.String(100), nullable=True, comment='关闭方式')
    engaged_seconds = db.Column(db.Integer, nullable=True, comment='响应投入(秒)')
    hours = db.Column(db.String(50), nullable=True, comment='告警时段')
    notifications = db.Column(db.Integer, nullable=True, default=0, comment='通知次数')
    interruptions = db.Column(db.Integer, nullable=True, default=0, comment='中断次数')
    acknowledgements = db.Column(db.Integer, nullable=True, default=0, comment='认领次数')
    assignments = db.Column(db.Integer, nullable=True, default=0, comment='分派次数')
    reassignments = db.Column(db.Integer, nullable=True, default=0, comment='重新分派次数')
    escalations = db.Column(db.Integer, nullable=True, default=0, comment='升级次数')
    manual_escalations = db.Column(db.Integer, nullable=True, default=0, comment='手动升级次数')
    timeout_escalations = db.Column(db.Integer, nullable=True, default=0, comment='自动升级次数')
    runbook_url = db.Column(db.String(500), nullable=True, comment='处理手册')
    rule_note = db.Column(db.Text, nullable=True, comment='规则说明/处理手册内容')
    scene = db.Column(db.String(200), nullable=True, comment='场景')
    
    raw_data = db.Column(db.Text, nullable=True, comment='原始JSON数据')
    
    created_at_db = db.Column(db.DateTime, default=datetime.now, comment='数据库记录创建时间')
    updated_at_db = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='数据库记录更新时间')

    def to_dict(self):
        return {
            'id': self.id,
            'incident_id': self.incident_id,
            'title': self.title,
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'severity': self.severity,
            'system_domain': self.system_domain,
            'sub_domain': self.sub_domain,
            'business_system': self.business_system,
            'progress': self.progress,
            'channel_id': self.channel_id,
            'channel_name': self.channel_name,
            'team_id': self.team_id,
            'team_name': self.team_name,
            'from_source': self.from_source,
            'created_at': self.created_at,
            'created_at_datetime': self.created_at_datetime.strftime('%Y-%m-%d %H:%M:%S') if self.created_at_datetime else None,
            'seconds_to_ack': self.seconds_to_ack,
            'seconds_to_close': self.seconds_to_close,
            'closed_by': self.closed_by,
            'engaged_seconds': self.engaged_seconds,
            'hours': self.hours,
            'notifications': self.notifications,
            'interruptions': self.interruptions,
            'acknowledgements': self.acknowledgements,
            'assignments': self.assignments,
            'reassignments': self.reassignments,
            'escalations': self.escalations,
            'manual_escalations': self.manual_escalations,
            'timeout_escalations': self.timeout_escalations,
            'runbook_url': self.runbook_url,
            'rule_note': self.rule_note,
            'scene': self.scene,
            'created_at_db': self.created_at_db.isoformat() if self.created_at_db else None,
            'updated_at_db': self.updated_at_db.isoformat() if self.updated_at_db else None
        }

    @staticmethod
    def from_api_data(item_data):
        def _to_int(value, default=None):
            if value is None or value == '':
                return default
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            try:
                return int(float(str(value).strip()))
            except (ValueError, TypeError):
                return default

        def _parse_timestamp(value):
            if value is None or value == '':
                return None

            # 纯数字时间戳（秒）
            parsed_int = _to_int(value)
            if parsed_int is not None:
                return parsed_int

            # 兼容 "2026-03-20T11:25:14" / "2026-03-20 11:25:14" 等格式
            if isinstance(value, str):
                raw = value.strip()
                if not raw:
                    return None

                # fromisoformat 支持 "YYYY-mm-ddTHH:MM:SS[.ffffff][+HH:MM]"
                iso_candidate = raw.replace('Z', '+00:00')
                try:
                    return int(datetime.fromisoformat(iso_candidate).timestamp())
                except ValueError:
                    pass

                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                    try:
                        return int(datetime.strptime(raw, fmt).timestamp())
                    except ValueError:
                        continue

            return None

        def _normalize_severity(value):
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

        def _extract_rule_note(labels_data, item):
            for key in ('rule_note', 'ruleNote', 'RuleNote'):
                value = labels_data.get(key)
                if value is not None and str(value).strip():
                    return str(value).strip()

            value = item.get('rule_note')
            if value is not None and str(value).strip():
                return str(value).strip()

            desc = item.get('description')
            if isinstance(desc, dict):
                for key in ('rule_note', 'ruleNote', 'RuleNote'):
                    value = desc.get(key)
                    if value is not None and str(value).strip():
                        return str(value).strip()
            elif isinstance(desc, str) and desc.strip():
                try:
                    parsed_desc = json.loads(desc)
                    if isinstance(parsed_desc, dict):
                        for key in ('rule_note', 'ruleNote', 'RuleNote'):
                            value = parsed_desc.get(key)
                            if value is not None and str(value).strip():
                                return str(value).strip()
                except Exception:
                    pass

            fields = item.get('fields')
            if isinstance(fields, dict):
                for key in ('rule_note', 'ruleNote', 'RuleNote'):
                    value = fields.get(key)
                    if value is not None and str(value).strip():
                        return str(value).strip()

            return None

        labels = item_data.get('labels', {})
        from_source = labels.get('From')
        source_normalized = str(from_source).strip().lower() if from_source is not None else ''
        
        # 获取原始的SystemDomain和SubDomain
        system_domain = labels.get('SystemDomain')
        sub_domain = labels.get('SubDomain')
        channel_name = str(item_data.get('channel_name') or '').strip()
        
        # 指定协作空间映射优先级最高：不区分来源，统一覆盖域/子域
        if channel_name == 'SRE协作空间':
            system_domain = 'SRE'
            sub_domain = 'SRE协作空间'
        elif channel_name == '稳定性平台':
            system_domain = 'SRE'
            sub_domain = '稳定性平台'
        # 根据from_source决定如何处理其余场景的system_domain和sub_domain
        elif source_normalized == 'zabbixinfra':
            # ZabbixInfra来源：直接从channel_name提取
            parts = channel_name.split('-')
            if len(parts) >= 2:
                system_domain = parts[0].strip()
                sub_domain = parts[1].strip()
        elif source_normalized == 'flashcat':
            # FlashCat来源：其它协作空间按原逻辑补齐
            if not system_domain or not sub_domain:
                parts = channel_name.split('-')
                if len(parts) >= 2:
                    if not system_domain:
                        system_domain = parts[0].strip()
                    if not sub_domain:
                        sub_domain = parts[1].strip()
        else:
            # 其他来源：遵循已有导入规则
            if not system_domain or not sub_domain:
                if channel_name == 'SRE协作空间':
                    system_domain = 'SRE'
                    sub_domain = 'SRE协作空间'
                elif channel_name == '稳定性平台':
                    system_domain = 'SRE'
                    sub_domain = '稳定性平台'
                else:
                    # 尝试从channel_name分割SystemDomain和SubDomain
                    parts = channel_name.split('-')
                    if len(parts) >= 2:
                        if not system_domain:
                            system_domain = parts[0].strip()
                        if not sub_domain:
                            sub_domain = parts[1].strip()

        created_at_ts = _parse_timestamp(item_data.get('created_at'))
        if created_at_ts is None:
            created_at_ts = _parse_timestamp(item_data.get('triggered_at'))
        rule_note = _extract_rule_note(labels, item_data)
        
        incident = Incident(
            incident_id=item_data.get('incident_id'),
            title=item_data.get('title'),
            rule_id=labels.get('rule_id'),
            rule_name=labels.get('rulename'),
            severity=_normalize_severity(item_data.get('severity')),
            system_domain=system_domain,
            sub_domain=sub_domain,
            business_system=labels.get('BusinessSystem'),
            progress=item_data.get('progress'),
            channel_id=str(item_data.get('channel_id', '')),
            channel_name=item_data.get('channel_name'),
            team_id=str(item_data.get('team_id', '')),
            team_name=item_data.get('team_name'),
            from_source=labels.get('From'),
            created_at=created_at_ts,
            seconds_to_ack=_to_int(item_data.get('seconds_to_ack')),
            seconds_to_close=_to_int(item_data.get('seconds_to_close')),
            closed_by=item_data.get('closed_by'),
            engaged_seconds=_to_int(item_data.get('engaged_seconds')),
            hours=item_data.get('hours'),
            notifications=_to_int(item_data.get('notifications'), 0),
            interruptions=_to_int(item_data.get('interruptions'), 0),
            acknowledgements=_to_int(item_data.get('acknowledgements'), 0),
            assignments=_to_int(item_data.get('assignments'), 0),
            reassignments=_to_int(item_data.get('reassignments'), 0),
            escalations=_to_int(item_data.get('escalations'), 0),
            manual_escalations=_to_int(item_data.get('manual_escalations'), 0),
            timeout_escalations=_to_int(item_data.get('timeout_escalations'), 0),
            runbook_url=labels.get('runbook_url') or item_data.get('runbook_url'),
            rule_note=rule_note,
            scene=labels.get('scene') or item_data.get('scene')
        )
        
        if incident.created_at:
            try:
                incident.created_at_datetime = datetime.fromtimestamp(incident.created_at)
            except (TypeError, ValueError, OSError):
                incident.created_at_datetime = None
        
        return incident


class AlertRule(db.Model):
    __tablename__ = 'alert_rules'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rule_id = db.Column(db.String(100), unique=True, nullable=False, comment='规则唯一标识符')
    rule_name = db.Column(db.String(500), nullable=True, comment='规则名称')
    rule_link = db.Column(db.String(500), nullable=True, comment='规则链接')
    runbook_url = db.Column(db.String(500), nullable=True, comment='处理手册链接')
    rule_note = db.Column(db.Text, nullable=True, comment='规则说明/处理手册内容')
    scene = db.Column(db.String(200), nullable=True, comment='关联场景（多场景用|分隔）')
    
    # 告警质量得分相关字段
    quality_score = db.Column(db.Float, nullable=True, comment='告警质量总得分')
    event_count_score = db.Column(db.Integer, nullable=True, comment='告警事件数得分')
    runbook_score = db.Column(db.Integer, nullable=True, comment='手册填写得分')
    scene_score = db.Column(db.Integer, nullable=True, comment='场景关联得分')
    invalid_rate_score = db.Column(db.Integer, nullable=True, comment='无效告警率得分')
    escalation_rate_score = db.Column(db.Integer, nullable=True, comment='自动升级率得分')
    mtta_rate_score = db.Column(db.Integer, nullable=True, comment='MTTA达标率得分')
    mttr_rate_score = db.Column(db.Integer, nullable=True, comment='MTTR达标率得分')
    jitter_rate_score = db.Column(db.Integer, nullable=True, comment='告警抖动率得分')
    
    # 得分计算时的原始数据（用于审计和调试）
    raw_event_count = db.Column(db.Integer, nullable=True, comment='原始告警事件数')
    raw_invalid_rate = db.Column(db.Float, nullable=True, comment='原始无效告警率')
    raw_escalation_rate = db.Column(db.Float, nullable=True, comment='原始自动升级率')
    raw_mtta_rate = db.Column(db.Float, nullable=True, comment='原始MTTA达标率')
    raw_mttr_rate = db.Column(db.Float, nullable=True, comment='原始MTTR达标率')
    raw_jitter_rate = db.Column(db.Float, nullable=True, comment='原始告警抖动率')
    
    quality_score_updated_at = db.Column(db.DateTime, nullable=True, comment='质量得分更新时间')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'rule_link': self.rule_link,
            'runbook_url': self.runbook_url,
            'rule_note': self.rule_note,
            'scene': self.scene,
            'quality_score': self.quality_score,
            'event_count_score': self.event_count_score,
            'runbook_score': self.runbook_score,
            'scene_score': self.scene_score,
            'invalid_rate_score': self.invalid_rate_score,
            'escalation_rate_score': self.escalation_rate_score,
            'mtta_rate_score': self.mtta_rate_score,
            'mttr_rate_score': self.mttr_rate_score,
            'jitter_rate_score': self.jitter_rate_score,
            'raw_event_count': self.raw_event_count,
            'raw_invalid_rate': self.raw_invalid_rate,
            'raw_escalation_rate': self.raw_escalation_rate,
            'raw_mtta_rate': self.raw_mtta_rate,
            'raw_mttr_rate': self.raw_mttr_rate,
            'raw_jitter_rate': self.raw_jitter_rate,
            'quality_score_updated_at': self.quality_score_updated_at.isoformat() if self.quality_score_updated_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @staticmethod
    def calculate_quality_score(rule_id, period_days=30, start_date=None, end_date=None, save_to_db=True):
        """
        计算单个规则的质量得分
        :param rule_id: 规则ID
        :param period_days: 统计周期天数
        :param start_date: 开始时间戳
        :param end_date: 结束时间戳
        :param save_to_db: 是否保存到数据库
        :return: 规则对象（已更新得分）
        """
        from datetime import datetime
        import math
        
        # 获取规则
        rule = AlertRule.query.filter_by(rule_id=rule_id).first()
        if not rule:
            return None
        
        # 构建查询条件
        query = Incident.query.filter_by(rule_id=rule_id)
        
        if start_date and end_date:
            query = query.filter(Incident.created_at >= start_date, Incident.created_at < end_date)
        else:
            # 默认使用最近period_days天的数据
            end_date = int(datetime.now().timestamp())
            start_date = end_date - period_days * 24 * 3600
            query = query.filter(Incident.created_at >= start_date, Incident.created_at < end_date)
        
        # 获取事件列表
        incidents = query.all()
        total_incidents = len(incidents)
        
        threshold_defaults = {
            'event_count': {'weight': 20, 'threshold_type': 'le', 'threshold_value': '1*period_days'},
            'runbook': {'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75'},
            'scene': {'weight': 10, 'threshold_type': 'ge', 'threshold_value': '60'},
            'invalid_rate': {'weight': 20, 'threshold_type': 'le', 'threshold_value': '20'},
            'escalation_rate': {'weight': 10, 'threshold_type': 'le', 'threshold_value': '5'},
            'mtta_rate': {'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75'},
            'mttr_rate': {'weight': 10, 'threshold_type': 'ge', 'threshold_value': '75'},
            'jitter_rate': {'weight': 10, 'threshold_type': 'le', 'threshold_value': '5'}
        }

        try:
            active_thresholds = ScoreThresholdConfig.query.filter_by(is_active=True).all()
            for cfg in active_thresholds:
                if cfg.dimension_key in threshold_defaults:
                    threshold_defaults[cfg.dimension_key] = {
                        'weight': cfg.weight,
                        'threshold_type': cfg.threshold_type,
                        'threshold_value': cfg.threshold_value
                    }
        except Exception:
            # 配置读取失败时降级为默认阈值，避免中断评分流程
            pass

        def _to_float(value, default_value=0.0):
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default_value)

        def _normalize_threshold_value(value):
            if isinstance(value, str):
                text = value.strip()
                if text == 'period_days':
                    return float(period_days)
                compact = text.replace(' ', '').lower()
                if compact.endswith('%'):
                    compact = compact[:-1]
                if compact.endswith('*period_days'):
                    factor = compact[:-len('*period_days')]
                    return _to_float(factor, 0.0) * float(period_days)
                if compact.startswith('period_days*'):
                    factor = compact[len('period_days*'):]
                    return _to_float(factor, 0.0) * float(period_days)
                if compact.endswith('xperiod_days'):
                    factor = compact[:-len('xperiod_days')]
                    return _to_float(factor, 0.0) * float(period_days)
                if compact.startswith('period_daysx'):
                    factor = compact[len('period_daysx'):]
                    return _to_float(factor, 0.0) * float(period_days)
                return _to_float(text, 0.0)
            return _to_float(value, 0.0)

        def _is_not_empty(value):
            return value is not None and str(value).strip() != ''

        def _passes_threshold(metric_key, metric_value):
            cfg = threshold_defaults.get(metric_key, {})
            threshold_type = (cfg.get('threshold_type') or '').strip().lower()
            threshold_value = cfg.get('threshold_value')

            if threshold_type == 'bool':
                val = str(threshold_value or '').strip().lower()
                if val in ['not_empty', 'non_empty', 'notempty', '非空', '1', 'true']:
                    return _is_not_empty(metric_value)
                if val in ['empty', '空', '0', 'false']:
                    return not _is_not_empty(metric_value)
                return bool(metric_value)

            current_value = _to_float(metric_value, 0.0)
            target_value = _normalize_threshold_value(threshold_value)

            if threshold_type == 'lt':
                return current_value < target_value
            if threshold_type == 'le':
                return current_value <= target_value
            if threshold_type == 'gt':
                return current_value > target_value
            if threshold_type == 'ge':
                return current_value >= target_value
            if threshold_type == 'eq':
                return current_value == target_value
            if threshold_type == 'ne':
                return current_value != target_value

            # 默认按不通过处理，避免未知阈值类型被误判为得分
            return False
        
        # 4. 无效告警率（无效告警数 / 总告警数，要求 <= 20%，得20分）
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
        
        invalid_rate = (invalid_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # 5. 自动升级率（升级次数 > 0的事件数 / 总事件数，要求 <= 10%，得10分）
        escalation_count = sum(1 for inc in incidents if inc.escalations and inc.escalations > 0)
        escalation_rate = (escalation_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # 6. MTTA达标率（MTTA <= 5分钟的事件数 / 总事件数，要求 >= 90%，得10分）
        mtta_ok_count = sum(1 for inc in incidents if inc.seconds_to_ack and inc.seconds_to_ack <= 300)
        mtta_rate = (mtta_ok_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # 7. MTTR达标率（MTTR <= 30分钟的事件数 / 总事件数，要求 >= 90%，得10分）
        mttr_ok_count = sum(1 for inc in incidents if inc.seconds_to_close and inc.seconds_to_close <= 1800)
        mttr_rate = (mttr_ok_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # 8. 告警抖动率（规则维度）
        # 口径：30min内重复发生的告警事件数 / 该规则在统计窗口内触发的告警事件总数
        # 说明：采用按时间排序后的滑动窗口统计（事件与其前一事件时间差 <= 1800 秒即记为重复事件）
        jitter_count = 0
        if total_incidents >= 2:
            valid_timestamps = []
            for inc in incidents:
                try:
                    ts = int(inc.created_at)
                except (TypeError, ValueError):
                    continue
                if ts > 0:
                    valid_timestamps.append(ts)
            valid_timestamps.sort()
            for i in range(1, len(valid_timestamps)):
                if valid_timestamps[i] - valid_timestamps[i - 1] <= 1800:
                    jitter_count += 1
        jitter_rate = (jitter_count / total_incidents * 100) if total_incidents > 0 else 0

        # 统一按阈值配置计算各维度得分
        event_count_score = threshold_defaults['event_count']['weight'] if _passes_threshold('event_count', total_incidents) else 0
        runbook_threshold_type = (threshold_defaults['runbook'].get('threshold_type') or '').strip().lower()
        scene_threshold_type = (threshold_defaults['scene'].get('threshold_type') or '').strip().lower()
        runbook_metric = rule.rule_note if runbook_threshold_type == 'bool' else (100 if _is_not_empty(rule.rule_note) else 0)
        scene_metric = rule.scene if scene_threshold_type == 'bool' else (100 if _is_not_empty(rule.scene) else 0)

        runbook_score = threshold_defaults['runbook']['weight'] if _passes_threshold('runbook', runbook_metric) else 0
        scene_score = threshold_defaults['scene']['weight'] if _passes_threshold('scene', scene_metric) else 0
        invalid_rate_score = threshold_defaults['invalid_rate']['weight'] if _passes_threshold('invalid_rate', invalid_rate) else 0
        escalation_rate_score = threshold_defaults['escalation_rate']['weight'] if _passes_threshold('escalation_rate', escalation_rate) else 0
        mtta_rate_score = threshold_defaults['mtta_rate']['weight'] if _passes_threshold('mtta_rate', mtta_rate) else 0
        mttr_rate_score = threshold_defaults['mttr_rate']['weight'] if _passes_threshold('mttr_rate', mttr_rate) else 0
        jitter_rate_score = threshold_defaults['jitter_rate']['weight'] if _passes_threshold('jitter_rate', jitter_rate) else 0
        
        # 计算总分
        total_score = sum([
            event_count_score,
            runbook_score,
            scene_score,
            invalid_rate_score,
            escalation_rate_score,
            mtta_rate_score,
            mttr_rate_score,
            jitter_rate_score
        ])
        
        # 更新规则得分
        rule.quality_score = round(total_score, 1)
        rule.event_count_score = event_count_score
        rule.runbook_score = runbook_score
        rule.scene_score = scene_score
        rule.invalid_rate_score = invalid_rate_score
        rule.escalation_rate_score = escalation_rate_score
        rule.mtta_rate_score = mtta_rate_score
        rule.mttr_rate_score = mttr_rate_score
        rule.jitter_rate_score = jitter_rate_score
        
        # 更新原始数据
        rule.raw_event_count = total_incidents
        rule.raw_invalid_rate = round(invalid_rate, 1)
        rule.raw_escalation_rate = round(escalation_rate, 1)
        rule.raw_mtta_rate = round(mtta_rate, 1)
        rule.raw_mttr_rate = round(mttr_rate, 1)
        rule.raw_jitter_rate = round(jitter_rate, 1)
        
        # 更新得分计算时间
        rule.quality_score_updated_at = datetime.now()
        
        # 根据参数决定是否保存到数据库
        if save_to_db:
            db.session.add(rule)
            db.session.commit()
        
        return rule


class CompletionCondition(db.Model):
    __tablename__ = 'completion_conditions'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False, comment='条件名称')
    type = db.Column(db.String(50), nullable=False, comment='条件类型(field_check/score_check/rate_check)')
    field = db.Column(db.String(100), nullable=False, comment='检查字段')
    value = db.Column(db.String(200), nullable=False, comment='条件值')
    logic = db.Column(db.String(10), default='AND', comment='逻辑关系(AND/OR)')
    guide = db.Column(db.Text, nullable=True, comment='操作指引')
    status = db.Column(db.String(20), default='enabled', comment='状态(enabled/disabled)')
    sort_order = db.Column(db.Integer, default=0, comment='排序顺序')
    created_at = db.Column(db.DateTime, default=datetime.now, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'field': self.field,
            'value': self.value,
            'logic': self.logic,
            'guide': self.guide,
            'status': self.status,
            'sort_order': self.sort_order,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
