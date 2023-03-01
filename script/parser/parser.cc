#include <stdio.h>
#include <stdlib.h>

#define NUM_THREAD 8
#define NUM_STATE 16
#define NUM_TSTATE (1<<NUM_STATE)
#define NUM_PART 10

  unsigned long int cur_time[NUM_STATE] = {0, };
  unsigned long int max_time[NUM_STATE][NUM_TSTATE] = {0, };
  unsigned long int cnt[NUM_TSTATE];
  unsigned long int sum[NUM_TSTATE][NUM_STATE] = {0, };
  unsigned long int max[NUM_STATE][NUM_TSTATE][NUM_STATE] = {0, };
  unsigned long int cnt_part[NUM_TSTATE][NUM_PART];
  unsigned long int sum_part[NUM_TSTATE][NUM_STATE][NUM_PART] = {0, };

// additional test code
  unsigned long int o_cnt = 0, e_cnt = 0;
  unsigned long int outlier[NUM_STATE] = {0, };
  unsigned long int expected[NUM_STATE] = {0, };

int main(int argc, char **argv) {
  FILE *fp;

  int tstate;

  int part_start[NUM_PART] = {76, 110, 170, 250, 380, 580, 870, 1300, 2000, 5000};
  int part_end[NUM_PART] = {110, 170, 250, 380, 580, 870, 1300, 2000, 5000, 0};

  fp = fopen(argv[1], "r");

  while (!feof(fp)) {
    tstate = 1;

    bool skip_run = false;
    for (int i = 0; i < NUM_STATE; i ++) {
      fscanf(fp, "%lu", &cur_time[i]);
      if (cur_time[i] != 111111) {
        tstate |= (1<<i);
      } else {
        cur_time[i] = 0;
      }

      if (cur_time[i] > 10000000) {
        cur_time[i] = 0;
      }
    }

    if (cur_time[NUM_STATE-1] == 0)
      continue; // skip all-zero case

    int part;
    if (cur_time[NUM_STATE - 1] < part_end[0]) {
      part = 0;
    } else if (cur_time[NUM_STATE - 1] < part_end[1]) {
      part = 1;
    } else if (cur_time[NUM_STATE - 1] < part_end[2]) {
      part = 2;
    } else if (cur_time[NUM_STATE - 1] < part_end[3]) {
      part = 3;
    } else if (cur_time[NUM_STATE - 1] < part_end[4]) {
      part = 4;
    } else if (cur_time[NUM_STATE - 1] < part_end[5]) {
      part = 5;
    } else if (cur_time[NUM_STATE - 1] < part_end[6]) {
      part = 6;
    } else if (cur_time[NUM_STATE - 1] < part_end[7]) {
      part = 7;
    } else if (cur_time[NUM_STATE - 1] < part_end[8]) {
      part = 8;
    } else {
      part = 9;
    }

    for (int j = 0; j < NUM_STATE; j ++) {
      int e = (j==NUM_STATE-1)?j:j+1;
      int s = (j==NUM_STATE-1)?0:j;
      if (max_time[j][tstate] < cur_time[e] - cur_time[s]) {
        max_time[j][tstate] = cur_time[e] - cur_time[s];
        for (int i = 0; i < NUM_STATE; i ++) {
          max[j][tstate][i] = cur_time[i];
        }
      }
    }

// additional debugging code
    if (tstate == 0xe9bf) {
      if (cur_time[11] - cur_time[8] > 1000) {
        o_cnt ++;
        for (int i = 0; i < NUM_STATE; i ++) {
          outlier[i] += cur_time[i];
        }
      } else {
        e_cnt ++;
        for (int i = 0; i < NUM_STATE; i ++) {
          expected[i] += cur_time[i];
        }
      }
    }

    cnt[tstate] ++;
    cnt_part[tstate][part] ++;
    for (int i = 0; i < NUM_STATE; i ++) {
      sum[tstate][i] += cur_time[i];
      sum_part[tstate][i][part] += cur_time[i];
      cur_time[i] = 0;
    }
  }

  for (int i = 0; i < NUM_TSTATE; i ++) {
    if (cnt[i] > 0) {
      printf("tstate %x count %lu\n", (unsigned int)i, cnt[i]);
      printf("time avg : ");
      for (int j = 0; j < NUM_STATE; j ++)
        printf("%7.2lf ", (double)sum[i][j] / cnt[i]);
      printf("\n");
      for (int k = 0; k < NUM_STATE; k ++) {
        printf("time max (%d-%d) : ", (k==NUM_STATE-1)?0:k, (k==NUM_STATE-1)?k:k+1);
        for (int j = 0; j < NUM_STATE; j ++)
          printf("%lu ", max[k][i][j]);
        printf("\n");
      }
      for (int k = 0; k < NUM_PART; k ++) {
        if (cnt_part[i][k] > 0) {
          printf("part %d ( %4d - %4d ) count %8lu time avg : ", k, part_start[k], part_end[k], cnt_part[i][k]);
          for (int j = 0; j < NUM_STATE; j ++)
            printf("%7.2lf ", (double)sum_part[i][j][k] / cnt_part[i][k]);
          printf("\n");
        }
      }
    }
  }


  printf("Extra debugging info\n");
  printf("Outlier cnt %lu\n", o_cnt);
  for (int i = 0; i < NUM_STATE; i ++)
    printf("%7.2lf ", (double)outlier[i] / o_cnt);
  printf("\n");
  printf("Expected cnt %lu\n", e_cnt);
  for (int i = 0; i < NUM_STATE; i ++)
    printf("%7.2lf ", (double)expected[i] / e_cnt);
  printf("\n");

  return 0;
}
