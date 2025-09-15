import pandas as pd
import numpy as np
import re
import sqlalchemy
from sqlalchemy import create_engine


class StandardData:
    def extract(self, path):
        return pd.read_csv(path, encoding="utf-8")

    def salary_usd_vnd(self, s):
        if pd.isna(s):
            return (np.nan, np.nan, None)

        s_lower = s.strip().lower()

        if "thoả thuận" in s_lower:
            return (np.nan, np.nan, None)

        if "usd" in s_lower:
            nums = re.findall(r"[\d,]+", s_lower)
            nums = [float(n.replace(",", "")) for n in nums]
            if "-" in s_lower:
                return (nums[0], nums[1], "USD")
            elif "trên" in s_lower:
                return (nums[0], np.nan, "USD")
            elif "tới" in s_lower:
                return (np.nan, nums[0], "USD")
            else:
                return (nums[0], nums[0], "USD")

        if "triệu" in s_lower:
            nums = re.findall(r"[\d,]+", s_lower)
            nums = [float(n.replace(",", "")) * 1_000_000 for n in nums]
            if "-" in s_lower:
                return (nums[0], nums[1], "VND")
            elif "trên" in s_lower:
                return (nums[0], np.nan, "VND")
            elif "tới" in s_lower:
                return (np.nan, nums[0], "VND")
            else:
                return (nums[0], nums[0], "VND")

        return (np.nan, np.nan, None)

    def expand_address(self, addr):
        if pd.isna(addr):
            return [(None, None)]
        parts = [p.strip() for p in str(addr).split(":") if p.strip() != ""]
        if len(parts) == 0:
            return [(None, None)]

        if len(parts) == 1:
            s = parts[0].lower()
            if "toàn quốc" in s:
                return [("Toàn quốc", None)]
            if "nước ngoài" in s:
                return [("Nước ngoài", None)]
            return [(parts[0], None)]

        pairs = []
        for i in range(0, len(parts), 2):
            city = parts[i]
            district = parts[i + 1] if i + 1 < len(parts) else None
            low = city.lower()
            if "toàn quốc" in low:
                pairs.append(("Toàn quốc", None))
            elif "nước ngoài" in low:
                pairs.append(("Nước ngoài", None))
            else:
                pairs.append((city, district))
        return pairs

    job_mapping = {
        # Business
        "business analyst": "Business",
        "ba": "Business",
        "product owner": "Business",
        "product manager": "Business",
        "project manager": "Business",
        "scrum master": "Business",
        "chuyên viên phân tích nghiệp vụ": "Business",
        "quản lý sản phẩm": "Business",
        "quản lý dự án": "Business",
        "manager": "Business",
        # IT
        "developer": "IT",
        "engineer": "IT",
        "programmer": "IT",
        "tester": "IT",
        "qa": "IT",
        "qc": "IT",
        "devops": "IT",
        "sysadmin": "IT",
        "administrator": "IT",
        "architect": "IT",
        "fullstack": "IT",
        "backend": "IT",
        "front end": "IT",
        "frontend": "IT",
        "mobile": "IT",
        "android": "IT",
        "ios": "IT",
        "java": "IT",
        "python": "IT",
        "php": "IT",
        "c++": "IT",
        "dotnet": "IT",
        "data": "IT",
        "machine learning": "IT",
        "ai": "IT",
        "bi": "IT",
        "big data": "IT",
        "intern": "IT",
        "thực tập sinh lập trình": "IT",
        "fresher": "IT",
        "lập trình": "IT",
        "it": "IT",
        "web designer": "IT"
    }

    def job_group(self, title):
        if pd.isna(title):
            return "Business"  # default
        t = title.strip().lower()
        for key, group in self.job_mapping.items():
            if key in t:
                return group
        return "Business"

    def transform(self, df):
        # Salary
        df[["min_salary", "max_salary", "salary_unit"]] = df["salary"].apply(
            lambda x: pd.Series(self.salary_usd_vnd(str(x)))
        )

        rows = []
        for idx, addr in df['address'].items():
            for city, district in self.expand_address(addr):
                row = df.loc[idx].to_dict()
                row["city"] = city
                row["district"] = district
                rows.append(row)
        df_expanded = pd.DataFrame(rows).reset_index(drop=True)

        df_expanded["job_group"] = df_expanded["job_title"].apply(self.job_group)

        df_clean = df_expanded[[
            "job_title", "job_group",
            "min_salary", "max_salary", "salary_unit",
            "city", "district"
        ]]

        return df_clean

    def load(self, df, db_url, table_name):
        engine = create_engine(
            db_url,
            connect_args={"charset": "utf8mb4"}
        )
        # Lưu dữ liệu vào MySQL
        df.to_sql(table_name, engine, if_exists="replace", index=False, dtype={
            "job_title": sqlalchemy.types.Text(),
            "job_group": sqlalchemy.types.Text(),
            "salary_unit": sqlalchemy.types.Text(),
            "city": sqlalchemy.types.Text(),
            "district": sqlalchemy.types.Text()
        })

        print(f"Loaded {len(df)} rows into {table_name}.")


if __name__ == "__main__":
    pipeline = StandardData()
    df = pipeline.extract(r"D:\Project DECK20\data.csv")
    df_clean = pipeline.transform(df)
    pipeline.load(
        df_clean,
        "mysql+pymysql://nguye:NguyE%401234@localhost:3306/job_data?charset=utf8mb4",
        "data"
    )


