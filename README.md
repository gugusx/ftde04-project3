# Project 3 - Data Automata

Proyek ini merupakan bagian dari kegiatan tim Data Automata yang bertujuan untuk membangun pipeline data end-to-end, mulai dari sumber OLTP hingga visualisasi hasil analisis di Google Sheet.

## 🎯 Business Use Cases
1. **User Purchase Period**  
   Menghitung berapa lama (dalam hari) pelanggan melakukan pembelian pertama sejak mendaftar (rata-rata).
2. **User Repurchase**  
   Menghitung berapa banyak pembelian yang telah dilakukan pelanggan (rata-rata).
3. **Revenue Period**  
   Menghitung total pendapatan per hari.

## 🛠️ Tools
- Python
- Google Sheets API
- ETL Scripts
- Data Warehouse (DWH)
- GitHub (versi kontrol)

## 🗂️ Struktur Data
### ERD OLTP
Struktur database operasional tempat data mentah berasal.

### ERD DWH
Struktur database warehouse yang digunakan untuk analisis.

## 🔄 Data Pipeline Overview
1. **Config File**  
   Berisi mapping antara struktur OLTP dan DWH.
2. **ETL Script**  
   Menyalin dan mentransformasi data dari OLTP ke DWH.
3. **Data Mart Scripts**  
   - User Purchase Period  
   - User Repurchase  
   - Revenue Period
4. **Upload Script ke Google Sheet**
5. **Automation Script**  
   Menjadwalkan dan menjalankan pipeline secara otomatis.

## 📈 Output
- Data Mart telah diunggah ke Google Sheets:
  - **Sheet 1**: User Purchase Period
  - **Sheet 2**: User Repurchase
  - **Sheet 3**: Revenue Period

## 📌 Langkah Selanjutnya
- Dokumentasi kode
- Monitoring pipeline otomatis
- Evaluasi hasil dan optimasi performa

---

> Proyek ini dibuat sebagai bagian dari proses pembelajaran dan pengembangan keterampilan data engineering & analisis bisnis.
