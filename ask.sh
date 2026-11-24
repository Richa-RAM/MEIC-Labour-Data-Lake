#!/usr/bin/env bash
# =====================================================================
#  Ask the data lake questions from the terminal.
#
#    ./ask.sh                      menu of business questions
#    ./ask.sh "SELECT ... ;"       run your own SQL
#    ./ask.sh shell                open a Hive SQL prompt (type !quit to leave)
# =====================================================================
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/config.env"
export PATH="$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH"
DB="labour_analytics"
DB_URL="${HIVE_JDBC_URL%/*}/$DB"

hive_sql() {   # hive_sql "<sql>"
  if [ "$HIVE_MODE" = "beeline" ]; then
    beeline -u "$DB_URL" -n "$HIVE_USER" --silent=true --showHeader=true \
            --outputformat=table2 -e "$1" 2>/dev/null
  else
    hive --database "$DB" -S -e "set hive.cli.print.header=true; $1" 2>/dev/null
  fi
}

# --- direct modes -----------------------------------------------------
if [ "${1:-}" = "shell" ]; then
  echo "Hive SQL prompt on database $DB. End each query with ; and type !quit to exit."
  if [ "$HIVE_MODE" = "beeline" ]; then beeline -u "$DB_URL" -n "$HIVE_USER"
  else hive --database "$DB"; fi
  exit
fi
if [ -n "${1:-}" ]; then
  time hive_sql "$1"
  exit
fi

# --- menu -------------------------------------------------------------
declare -a TITLE SQL
TITLE[1]="Show all tables";                      SQL[1]="SHOW TABLES;"
TITLE[2]="Record counts: raw vs clean vs rejected"
SQL[2]="SELECT 'raw' AS zone, COUNT(*) AS records FROM labour_raw UNION ALL SELECT 'clean', COUNT(*) FROM labour_cleaned UNION ALL SELECT 'rejected', COUNT(*) FROM labour_rejected;"
TITLE[3]="Top 10 client projects by labour cost"
SQL[3]="SELECT project, total_labour_cost, total_working_hours, distinct_employees, cost_per_hour FROM project_costs WHERE is_overhead = false ORDER BY total_labour_cost DESC LIMIT 10;"
TITLE[4]="Overhead vs client project cost"
SQL[4]="SELECT CASE WHEN is_overhead THEN 'Overhead' ELSE 'Client projects' END AS cost_type, ROUND(SUM(total_labour_cost),2) AS labour_cost, ROUND(100*SUM(total_labour_cost)/SUM(SUM(total_labour_cost)) OVER (),1) AS pct FROM project_costs GROUP BY CASE WHEN is_overhead THEN 'Overhead' ELSE 'Client projects' END;"
TITLE[5]="Top 10 employees by hours (with activity)"
SQL[5]="SELECT employee, type_of_work, total_working_hours, days_worked, total_labour_cost FROM employee_work_hours ORDER BY total_working_hours DESC LIMIT 10;"
TITLE[6]="Hours and cost by type of work"
SQL[6]="SELECT type_of_work, ROUND(SUM(total_working_hours),2) AS hours, ROUND(SUM(total_labour_cost),2) AS labour_cost FROM employee_work_hours GROUP BY type_of_work ORDER BY hours DESC LIMIT 15;"
TITLE[7]="Data-quality summary (rejected and flagged counts)"
SQL[7]="SELECT check_name, record_count FROM data_quality_summary;"
TITLE[8]="Rejected records and why"
SQL[8]="SELECT record_id, work_date, employee, project, total_hours, amount, reject_reason FROM labour_rejected ORDER BY total_hours DESC LIMIT 20;"
TITLE[9]="Employees with the most overtime days (>12 h)"
SQL[9]="SELECT employee, designation, COUNT(*) AS overtime_days, MAX(total_hours) AS max_hours FROM flagged_records WHERE flag_overtime = true GROUP BY employee, designation ORDER BY overtime_days DESC LIMIT 10;"
TITLE[10]="Labour cost trend by year"
SQL[10]="SELECT year, total_labour_cost, total_working_hours, distinct_employees, months_covered, overhead_cost, cost_per_hour FROM yearly_trends ORDER BY year;"

while true; do
  echo
  echo "======== Middle East Infra Contracting - Labour Data Lake ========"
  for i in $(seq 1 10); do printf "  %2d) %s\n" "$i" "${TITLE[$i]}"; done
  echo "   p) Preview any table (first 10 rows)"
  echo "   d) Describe a table (columns and types)"
  echo "   e) Monthly cost for a year you choose"
  echo "   c) Costs for a project you choose (search by name)"
  echo "   s) Type your own SQL"
  echo "   q) Quit"
  read -rp "Choose: " choice
  case "$choice" in
    q|Q) exit 0 ;;
    p) read -rp "Table name: " t; time hive_sql "SELECT * FROM $t LIMIT 10;" ;;
    d) read -rp "Table name: " t; hive_sql "DESCRIBE $t;" ;;
    e) read -rp "Year (2017-2026): " y
       time hive_sql "SELECT month, ROUND(SUM(amount),2) AS labour_cost, ROUND(SUM(total_hours),2) AS hours, COUNT(*) AS records FROM labour_cleaned WHERE year = $y GROUP BY month ORDER BY month;" ;;
    c) read -rp "Part of the project name or number (e.g. SHARJAH or 448): " p
       time hive_sql "SELECT project, total_labour_cost, total_working_hours, distinct_employees, first_work_date, last_work_date FROM project_costs WHERE UPPER(project) LIKE UPPER('%$p%') ORDER BY total_labour_cost DESC LIMIT 20;" ;;
    s) read -rp "SQL> " q; time hive_sql "$q" ;;
    *) if [[ "$choice" =~ ^[0-9]+$ ]] && [ -n "${SQL[$choice]:-}" ]; then
         echo ">> ${TITLE[$choice]}"; time hive_sql "${SQL[$choice]}"
       else echo "Unknown choice"; fi ;;
  esac
done
